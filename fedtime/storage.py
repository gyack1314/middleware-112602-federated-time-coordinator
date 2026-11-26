"""SQLite 持久层。

每条输入命令及其处理结果写入只追加的事件日志；领域状态同时物化为表，
重启后从物化状态重建。一次命令的所有状态变更在同一个事务内提交，
保证重启后仍按原逻辑时刻接续。
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .model import (
    Edge,
    Endpoint,
    Grant,
    Message,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    at INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    result TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run (
    name TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS endpoint (
    run TEXT NOT NULL,
    name TEXT NOT NULL,
    boot INTEGER NOT NULL,
    status TEXT NOT NULL,
    applied INTEGER NOT NULL,
    granted INTEGER NOT NULL,
    request_target INTEGER,
    request_ref TEXT,
    pending_target INTEGER,
    last_used INTEGER NOT NULL,
    PRIMARY KEY (run, name)
);
CREATE TABLE IF NOT EXISTS edge (
    run TEXT NOT NULL,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    state TEXT NOT NULL,
    boundary INTEGER NOT NULL,
    promise_lower INTEGER,
    promise_boot INTEGER,
    retired_at INTEGER,
    PRIMARY KEY (run, sender, receiver)
);
CREATE TABLE IF NOT EXISTS message (
    seq INTEGER PRIMARY KEY,
    run TEXT NOT NULL,
    event TEXT NOT NULL,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    tick INTEGER NOT NULL,
    boot INTEGER NOT NULL,
    state TEXT NOT NULL,
    grant_seq INTEGER,
    delivered_order INTEGER
);
CREATE TABLE IF NOT EXISTS grant (
    seq INTEGER PRIMARY KEY,
    run TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    tick INTEGER NOT NULL,
    boot INTEGER NOT NULL,
    request_ref TEXT NOT NULL,
    state TEXT NOT NULL,
    checkpoint INTEGER
);
CREATE TABLE IF NOT EXISTS grant_message (
    grant_seq INTEGER NOT NULL,
    message_seq INTEGER NOT NULL,
    ord INTEGER NOT NULL,
    PRIMARY KEY (grant_seq, message_seq)
);
"""


class Storage:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self):
        columns = {
            r["name"] for r in self.conn.execute(
                "PRAGMA table_info(endpoint)")}
        if "pending_target" not in columns:
            self.conn.execute(
                "ALTER TABLE endpoint ADD COLUMN pending_target INTEGER")

    def transaction(self):
        return self.conn  # 由 engine 用显式 BEGIN/COMMIT 控制

    def close(self):
        self.conn.close()

    # -- 日志 ----------------------------------------------------------------
    def append_log(self, at, kind, payload, result):
        self.conn.execute(
            "INSERT INTO event_log(at, kind, payload, result) VALUES (?,?,?,?)",
            (at, kind, json.dumps(payload, ensure_ascii=False, sort_keys=True),
             json.dumps(result, ensure_ascii=False, sort_keys=True)),
        )

    def read_log(self):
        rows = self.conn.execute(
            "SELECT at, kind, payload, result FROM event_log ORDER BY seq"
        ).fetchall()
        return [
            {"at": r["at"], "kind": r["kind"],
             "record": json.loads(r["payload"]), "result": json.loads(r["result"])}
            for r in rows
        ]

    # -- run / endpoint ------------------------------------------------------
    def ensure_run(self, run):
        self.conn.execute(
            "INSERT OR IGNORE INTO run(name) VALUES (?)", (run,))

    def upsert_endpoint(self, ep: Endpoint):
        self.conn.execute(
            """INSERT INTO endpoint(run, name, boot, status, applied, granted,
                                    request_target, request_ref,
                                    pending_target, last_used)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run, name) DO UPDATE SET
                 boot=excluded.boot, status=excluded.status,
                 applied=excluded.applied, granted=excluded.granted,
                 request_target=excluded.request_target,
                 request_ref=excluded.request_ref,
                 pending_target=excluded.pending_target,
                 last_used=excluded.last_used""",
            (ep.run, ep.name, ep.boot, ep.status, ep.applied, ep.granted,
             ep.request_target, ep.request_ref, ep.pending_target,
             ep.last_used),
        )

    def upsert_edge(self, edge: Edge):
        self.conn.execute(
            """INSERT INTO edge(run, sender, receiver, state, boundary,
                                promise_lower, promise_boot, retired_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(run, sender, receiver) DO UPDATE SET
                 state=excluded.state, boundary=excluded.boundary,
                 promise_lower=excluded.promise_lower,
                 promise_boot=excluded.promise_boot,
                 retired_at=excluded.retired_at""",
            (edge.run, edge.sender, edge.receiver, edge.state, edge.boundary,
             edge.promise_lower, edge.promise_boot, edge.retired_at),
        )

    def insert_message(self, msg: Message):
        self.conn.execute(
            """INSERT INTO message(seq, run, event, sender, receiver, tick,
                                   boot, state, grant_seq, delivered_order)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (msg.seq, msg.run, msg.event, msg.sender, msg.receiver, msg.tick,
             msg.boot, msg.state, msg.grant_seq, msg.delivered_order),
        )

    def update_message(self, msg: Message):
        self.conn.execute(
            """UPDATE message SET state=?, grant_seq=?, delivered_order=?
               WHERE seq=?""",
            (msg.state, msg.grant_seq, msg.delivered_order, msg.seq),
        )

    def insert_grant(self, grant: Grant):
        self.conn.execute(
            """INSERT INTO grant(seq, run, endpoint, tick, boot, request_ref,
                                 state, checkpoint)
               VALUES (?,?,?,?,?,?,?,?)""",
            (grant.seq, grant.run, grant.endpoint, grant.tick, grant.boot,
             grant.request_ref, grant.state, grant.checkpoint),
        )
        for order, message_seq in enumerate(grant.messages):
            self.conn.execute(
                """INSERT INTO grant_message(grant_seq, message_seq, ord)
                   VALUES (?,?,?)""",
                (grant.seq, message_seq, order),
            )

    def settle_grant(self, seq, checkpoint):
        self.conn.execute(
            "UPDATE grant SET state=?, checkpoint=? WHERE seq=?",
            ("settled", checkpoint, seq),
        )

    def set_meta(self, key, value):
        self.conn.execute(
            """INSERT INTO meta(key, value) VALUES (?,?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, str(value)),
        )

    def get_meta(self, key, default=None):
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    # -- 重建 ----------------------------------------------------------------
    def load_all(self):
        endpoints = {
            (r["run"], r["name"]): Endpoint(
                run=r["run"], name=r["name"], boot=r["boot"],
                status=r["status"], applied=r["applied"], granted=r["granted"],
                request_target=r["request_target"], request_ref=r["request_ref"],
                pending_target=r["pending_target"],
                last_used=r["last_used"])
            for r in self.conn.execute("SELECT * FROM endpoint")
        }
        edges = {
            (r["run"], r["sender"], r["receiver"]): Edge(
                run=r["run"], sender=r["sender"], receiver=r["receiver"],
                state=r["state"], boundary=r["boundary"],
                promise_lower=r["promise_lower"], promise_boot=r["promise_boot"],
                retired_at=r["retired_at"])
            for r in self.conn.execute("SELECT * FROM edge")
        }
        messages = {
            r["seq"]: Message(
                seq=r["seq"], run=r["run"], event=r["event"],
                sender=r["sender"], receiver=r["receiver"], tick=r["tick"],
                boot=r["boot"], state=r["state"], grant_seq=r["grant_seq"],
                delivered_order=r["delivered_order"])
            for r in self.conn.execute("SELECT * FROM message")
        }
        grants = {}
        for r in self.conn.execute("SELECT * FROM grant"):
            linked = [
                row["message_seq"]
                for row in self.conn.execute(
                    "SELECT message_seq FROM grant_message WHERE grant_seq=? "
                    "ORDER BY ord", (r["seq"],))
            ]
            grants[r["seq"]] = Grant(
                seq=r["seq"], run=r["run"], endpoint=r["endpoint"],
                tick=r["tick"], boot=r["boot"], request_ref=r["request_ref"],
                state=r["state"], checkpoint=r["checkpoint"], messages=linked)
        return endpoints, edges, messages, grants
