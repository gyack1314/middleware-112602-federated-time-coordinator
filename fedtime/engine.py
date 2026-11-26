"""协调引擎：保守时间管理与因果事件交付。

核心不变量
----------
* 授予端点的时间不会越过任何一条已入场依赖边的承诺排他下界；
  边界 tick 上的已确定事件必须随授予一起交付，端点越过该 tick 前
  必然已经处理完这个时刻的全部消息。
* 同一端点同一时刻只有一个未应用授予（fence），授予带 boot 代次，
  旧代次进程无法确认新授予，同一授予不能被两个进程执行。
* 新增/重启用依赖边先停在 joining，发送端承诺越过入场边界后才激活，
  已有参与者的授予不会因此倒退。
* 所有命令在一个事务内完成状态变更并持久化。
"""

from __future__ import annotations

import threading
from collections import defaultdict

from .model import (
    EDGE_ACTIVE,
    EDGE_JOINING,
    EDGE_RETIRED,
    EDGE_RETIRING,
    ENDPOINT_EXITED,
    ENDPOINT_LIVE,
    GRANT_OPEN,
    GRANT_SETTLED,
    MSG_BUFFERED,
    MSG_DELIVERED,
    MSG_PENDING,
    Endpoint,
    Edge,
    Grant,
    Message,
    require_int,
)
from .storage import Storage


class ProtocolError(Exception):
    """参与方违反联调契约（时间倒退、旧代次、重复确认等）。"""


def _require_str(value, name):
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"{name} 必须是非空字符串")
    return value


class Coordinator:
    def __init__(self, storage: Storage):
        self.db = storage
        self.lock = threading.RLock()
        self.endpoints, self.edges, self.messages, self.grants = storage.load_all()
        self._seq_cache = {
            "message": max((m.seq for m in self.messages.values()), default=0),
            "grant": max((g.seq for g in self.grants.values()), default=0),
        }
        # 便于按维度检索的内存索引
        self.inbound = defaultdict(list)   # (run, receiver) -> Edge
        self.outbound = defaultdict(list)  # (run, sender) -> Edge
        self.held = defaultdict(list)      # (run, receiver) -> Message
        for edge in self.edges.values():
            self._index_edge(edge)
        for msg in self.messages.values():
            if msg.state in (MSG_PENDING, MSG_BUFFERED, MSG_DELIVERED):
                self.held[(msg.run, msg.receiver)].append(msg)
        for key in self.held:
            self.held[key].sort(key=lambda m: (m.tick, m.seq))

    # ------------------------------------------------------------------ 索引
    def _index_edge(self, edge: Edge):
        self.inbound[(edge.run, edge.receiver)].append(edge)
        self.outbound[(edge.run, edge.sender)].append(edge)

    def _endpoint(self, run, name) -> Endpoint:
        ep = self.endpoints.get((run, name))
        if ep is None:
            raise ProtocolError(f"端点 {name} 尚未加入 run {run}")
        return ep

    def _edge(self, run, sender, receiver) -> Edge:
        edge = self.edges.get((run, sender, receiver))
        if edge is None:
            raise ProtocolError(f"依赖边 {sender}->{receiver} 不存在")
        return edge

    def _next_seq(self, kind) -> int:
        self._seq_cache[kind] += 1
        return self._seq_cache[kind]

    # ------------------------------------------------------------- 命令入口
    def process(self, at, kind, record):
        """处理一条参与方消息，返回该消息的处理结果。"""
        if type(at) is not int or at < 0:
            raise ValueError("at 必须是非负整数")
        if not isinstance(record, dict):
            raise ProtocolError("record 必须是对象")
        handler = {
            "join": self._join,
            "edge": self._edge_cmd,
            "event": self._event,
            "promise": self._promise,
            "advance": self._advance,
            "applied": self._applied,
            "restart": self._restart,
            "inspect": self._inspect,
        }.get(kind)
        if handler is None:
            raise ProtocolError(f"未登记的消息名称 {kind}")
        with self.lock:
            self.db.conn.execute("BEGIN IMMEDIATE")
            try:
                result = handler(at, record)
                self.db.append_log(at, kind, record, result)
                self.db.conn.commit()
            except Exception:
                self.db.conn.rollback()
                raise
        return result

    # ------------------------------------------------------------------ join
    def _join(self, at, record):
        run = _require_str(record.get("run"), "run")
        name = _require_str(record.get("endpoint"), "endpoint")
        boot = require_int(record.get("boot"), "boot")
        applied = require_int(record.get("applied"), "applied")
        self.db.ensure_run(run)
        existing = self.endpoints.get((run, name))
        if existing is None:
            ep = Endpoint(run=run, name=name, boot=boot, applied=applied,
                          granted=applied, last_used=at)
            self.endpoints[(run, name)] = ep
            self.db.upsert_endpoint(ep)
            return {"joined": name, "boot": boot, "mode": "new",
                    "resume": self._resume_bundle(ep)}
        # 同名端点重新加入
        if boot < existing.boot:
            raise ProtocolError(
                f"代次 {boot} 已过期，当前代次为 {existing.boot}")
        if applied < existing.applied:
            raise ProtocolError(
                f"实际位置 {applied} 小于已持久化位置 {existing.applied}，"
                "旧进度不得回退服务状态")
        if boot == existing.boot and existing.status == ENDPOINT_LIVE:
            # 同代次重连：仅重发未应用的授予与消息
            return {"joined": name, "boot": boot, "mode": "reconnect",
                    "resume": self._resume_bundle(existing)}
        if existing.status == ENDPOINT_EXITED and boot == existing.boot:
            raise ProtocolError(
                "退出的进程必须以更大的 boot 重新加入")
        # 新代次接棒
        if applied > existing.granted:
            raise ProtocolError(
                f"实际位置 {applied} 越过了已授予位置 {existing.granted}")
        old_boot = existing.boot
        existing.boot = boot
        existing.status = ENDPOINT_LIVE
        existing.applied = applied
        existing.granted = max(existing.granted, applied)
        existing.request_target = None
        existing.request_ref = None
        if existing.pending_target is not None \
                and existing.pending_target <= applied:
            existing.pending_target = None
        existing.last_used = at
        self.db.upsert_endpoint(existing)
        # 旧代次未应用的授予不能再被执行：作废后按新 boot 重签同一时刻。
        # 重签必须在旧承诺作废之前完成，否则边退回 joining 会阻塞泵。
        reissued = None
        own_open = [
            g for g in self.grants.values()
            if g.run == run and g.endpoint == name
            and g.state == GRANT_OPEN]
        reissue_target = max(
            (g.tick for g in own_open if g.tick > applied), default=None)
        for old_open in own_open:
            if old_open.tick <= applied:
                continue
            old_open.state = "superseded"
            self.db.conn.execute(
                "UPDATE grant SET state=? WHERE seq=?",
                (old_open.state, old_open.seq))
            for seq in old_open.messages:
                msg = self.messages[seq]
                msg.state = MSG_BUFFERED
                msg.grant_seq = None
                self.db.update_message(msg)
        if reissue_target is not None:
            existing.granted = applied
            # 重签需要推进目标：恢复为被作废授予的时刻与请求引用
            existing.request_target = reissue_target
            existing.request_ref = own_open[0].request_ref
            self.db.upsert_endpoint(existing)
            early_pump = self._pump(run)
            reissued = next(
                (g for g in early_pump["granted"] if g["endpoint"] == name),
                None)
        # 旧代次的承诺作废，边退回到等待承诺的入场/阻塞状态
        for edge in self.inbound[(run, name)]:
            if edge.promise_boot == old_boot:
                edge.promise_lower = None
                edge.promise_boot = None
                if edge.state == EDGE_ACTIVE:
                    edge.state = EDGE_JOINING
                    edge.boundary = max(edge.boundary, existing.granted)
                self.db.upsert_edge(edge)
        for edge in self.outbound[(run, name)]:
            if edge.promise_boot == old_boot:
                edge.promise_lower = None
                edge.promise_boot = None
                if edge.state == EDGE_ACTIVE:
                    edge.state = EDGE_JOINING
                    edge.boundary = max(edge.boundary, self._endpoint(
                        run, edge.receiver).granted)
                self.db.upsert_edge(edge)
        # 旧代次暂存的待入场消息作废（承诺已失效，需新代次重新发送）
        for msg in self.held[(run, name)]:
            if msg.boot == old_boot and msg.state == MSG_PENDING:
                msg.state = "stale"
                self.db.update_message(msg)
        pump = self._pump(run)
        return {"joined": name, "boot": boot, "mode": "new-generation",
                "previous_boot": old_boot,
                "reissued_grant": reissued,
                "resume": self._resume_bundle(existing), "pump": pump}

    def _resume_bundle(self, ep: Endpoint):
        """重发未应用的授予与尚未处理的保管消息。"""
        grant = None
        if ep.has_open_grant:
            grant = next(
                (g for g in self.grants.values()
                 if g.run == ep.run and g.endpoint == ep.name
                 and g.state == GRANT_OPEN), None)
            if grant is not None:
                grant = self._grant_view(grant)
        pending = [
            self._message_view(m)
            for m in self.held[(ep.run, ep.name)]
            if m.state in (MSG_BUFFERED, MSG_DELIVERED)
        ]
        return {"endpoint": ep.name, "boot": ep.boot,
                "applied": ep.applied, "granted": ep.granted,
                "open_grant": grant, "outstanding_events": pending}

    # ------------------------------------------------------------------ edge
    def _edge_cmd(self, at, record):
        run = _require_str(record.get("run"), "run")
        sender = _require_str(record.get("from"), "from")
        receiver = _require_str(record.get("to"), "to")
        if sender == receiver:
            raise ProtocolError("不支持自环依赖边")
        active = record.get("active")
        if not isinstance(active, bool):
            raise ProtocolError("active 必须是布尔值")
        self._endpoint(run, sender)
        receiver_ep = self._endpoint(run, receiver)
        key = (run, sender, receiver)
        edge = self.edges.get(key)
        if active:
            if edge is None:
                edge = Edge(run=run, sender=sender, receiver=receiver,
                            state=EDGE_JOINING,
                            boundary=receiver_ep.granted)
                self.edges[key] = edge
                self._index_edge(edge)
            elif edge.state == EDGE_RETIRED:
                # 重新启用：不能假定新端已走到下游当前授权位置
                edge.state = EDGE_JOINING
                edge.boundary = max(edge.boundary, receiver_ep.granted)
                edge.retired_at = None
            elif edge.state == EDGE_JOINING:
                edge.boundary = max(edge.boundary, receiver_ep.granted)
            elif edge.state in (EDGE_ACTIVE, EDGE_RETIRING):
                if edge.state == EDGE_RETIRING:
                    edge.state = EDGE_ACTIVE
            self.db.upsert_edge(edge)
            result = {"edge": f"{sender}->{receiver}", "state": edge.state,
                      "boundary": edge.boundary}
            if edge.state == EDGE_JOINING:
                result["required"] = (
                    f"{sender} 需承诺 lower > {edge.boundary}（即 lower >= "
                    f"{edge.boundary + 1}）后方可入场")
            pump = self._pump(run)
            result["pump"] = pump
            return result
        # 停用申请
        if edge is None:
            raise ProtocolError("无法停用不存在的依赖边")
        if edge.state == EDGE_JOINING:
            edge.state = EDGE_RETIRED
            edge.retired_at = at
            for msg in self.held[(run, receiver)]:
                if (msg.sender == sender and msg.state == MSG_PENDING):
                    msg.state = "rejected"
                    self.db.update_message(msg)
        elif edge.state == EDGE_ACTIVE:
            edge.state = EDGE_RETIRING
        elif edge.state == EDGE_RETIRING:
            pass
        self.db.upsert_edge(edge)
        pump = self._pump(run)
        return {"edge": f"{sender}->{receiver}", "state": edge.state,
                "pump": pump}

    # --------------------------------------------------------------- promise
    def _promise(self, at, record):
        run = _require_str(record.get("run"), "run")
        sender = _require_str(record.get("from"), "from")
        receiver = _require_str(record.get("to"), "to")
        boot = require_int(record.get("boot"), "boot")
        lower = require_int(record.get("lower"), "lower")
        ep = self._endpoint(run, sender)
        if ep.status == ENDPOINT_EXITED:
            raise ProtocolError("端点已退出，不能再作出承诺")
        if boot != ep.boot:
            raise ProtocolError(
                f"承诺代次 {boot} 与当前代次 {ep.boot} 不符，"
                "只有当前 boot 的承诺可作授权依据")
        edge = self._edge(run, sender, receiver)
        if edge.state == EDGE_RETIRED:
            raise ProtocolError("依赖边已退役，不再受理承诺")
        if edge.promise_boot == boot and edge.promise_lower is not None \
                and lower < edge.promise_lower:
            raise ProtocolError(
                f"承诺只能单调推进：现有 lower={edge.promise_lower}")
        edge.promise_lower = lower
        edge.promise_boot = boot
        self.db.upsert_edge(edge)
        pump = self._pump(run)
        return {"edge": f"{sender}->{receiver}", "lower": lower,
                "state": edge.state, "pump": pump}

    # ----------------------------------------------------------------- event
    def _event(self, at, record):
        run = _require_str(record.get("run"), "run")
        event = _require_str(record.get("event"), "event")
        sender = _require_str(record.get("from"), "from")
        receiver = _require_str(record.get("to"), "to")
        boot = require_int(record.get("boot"), "boot")
        tick = require_int(record.get("tick"), "tick")
        sender_ep = self._endpoint(run, sender)
        self._endpoint(run, receiver)
        if sender_ep.status == ENDPOINT_EXITED:
            raise ProtocolError("端点已退出，不能再发送事件")
        if boot != sender_ep.boot:
            raise ProtocolError(
                f"事件代次 {boot} 与发送端当前代次 {sender_ep.boot} 不符")
        edge = self._edge(run, sender, receiver)
        if edge.state == EDGE_RETIRED:
            raise ProtocolError("依赖边已退役，事件不予受理")
        if edge.state == EDGE_RETIRING:
            raise ProtocolError("依赖边正在停用，停止受理新的来源消息")
        msg = Message(seq=self._next_seq("message"), run=run, event=event,
                      sender=sender, receiver=receiver, tick=tick, boot=boot)
        # 入场判定：joining 边上的消息先暂存，承诺覆盖后再入场
        if edge.state == EDGE_JOINING:
            if edge.promise_boot == boot and edge.promise_lower is not None \
                    and tick < edge.promise_lower:
                raise ProtocolError(
                    f"tick {tick} 小于已承诺排他下界 {edge.promise_lower}，"
                    "与发送端自身承诺矛盾")
            msg.state = MSG_PENDING
        else:
            if edge.promise_boot == boot and edge.promise_lower is not None \
                    and tick < edge.promise_lower:
                raise ProtocolError(
                    f"tick {tick} 小于已承诺排他下界 {edge.promise_lower}，"
                    "与发送端自身承诺矛盾")
            msg.state = MSG_BUFFERED
        self.messages[msg.seq] = msg
        self.held[(run, receiver)].append(msg)
        self.held[(run, receiver)].sort(key=lambda m: (m.tick, m.seq))
        self.db.insert_message(msg)
        pump = self._pump(run)
        return {"event_seq": msg.seq, "event": event, "tick": tick,
                "from": sender, "to": receiver, "state": msg.state,
                "pump": pump}

    # --------------------------------------------------------------- advance
    def _advance(self, at, record):
        run = _require_str(record.get("run"), "run")
        name = _require_str(record.get("endpoint"), "endpoint")
        target = require_int(record.get("target"), "target")
        request = _require_str(record.get("request"), "request")
        ep = self._endpoint(run, name)
        if ep.status == ENDPOINT_EXITED:
            raise ProtocolError("端点已退出，不能请求推进")
        if target < ep.applied:
            raise ProtocolError(
                f"目标 {target} 小于已应用位置 {ep.applied}")
        ep.request_target = target
        ep.request_ref = request
        ep.pending_target = target
        ep.last_used = at
        self.db.upsert_endpoint(ep)
        pump = self._pump(run)
        return self._advance_result(ep, pump)

    def _advance_result(self, ep: Endpoint, pump):
        result = {"endpoint": ep.name, "request_target": ep.request_target,
                  "applied": ep.applied, "granted": ep.granted, "pump": pump}
        if ep.has_open_grant:
            result["queued"] = "存在未应用授予，请求已登记，待确认后处理"
        waiting = pump.get("waiting", {}).get(ep.name)
        if waiting:
            result["waiting"] = waiting
        return result

    # --------------------------------------------------------------- applied
    def _applied(self, at, record):
        run = _require_str(record.get("run"), "run")
        name = _require_str(record.get("endpoint"), "endpoint")
        boot = require_int(record.get("boot"), "boot")
        grant_seq = require_int(record.get("grant"), "grant")
        checkpoint = require_int(record.get("checkpoint"), "checkpoint")
        events = record.get("events")
        if not isinstance(events, list):
            raise ProtocolError("events 必须是消息 seq 列表")
        if any(type(x) is not int for x in events):
            raise ProtocolError("events 只能包含整数 seq")
        ep = self._endpoint(run, name)
        if ep.status == ENDPOINT_EXITED:
            raise ProtocolError("端点已退出，不能确认应用")
        if boot != ep.boot:
            raise ProtocolError(
                f"确认代次 {boot} 与当前代次 {ep.boot} 不符，"
                "旧进程不能确认当前授予")
        grant = self.grants.get(grant_seq)
        if grant is None or grant.run != run or grant.endpoint != name:
            raise ProtocolError(f"授予 {grant_seq} 不属于该端点")
        if grant.state != GRANT_OPEN:
            raise ProtocolError(f"授予 {grant_seq} 已应用，不得重复确认")
        if grant.boot != boot:
            raise ProtocolError(
                f"授予 {grant_seq} 签发给代次 {grant.boot}，"
                f"代次 {boot} 无权执行")
        if checkpoint != grant.tick:
            raise ProtocolError(
                f"checkpoint {checkpoint} 与授予时刻 {grant.tick} 不一致")
        if sorted(events) != sorted(grant.messages):
            raise ProtocolError(
                "applied 的事件集合必须与授予交付的消息完全一致")
        grant.state = GRANT_SETTLED
        grant.checkpoint = checkpoint
        self.db.settle_grant(grant.seq, checkpoint)
        for seq in grant.messages:
            msg = self.messages[seq]
            msg.state = "applied"
            msg.grant_seq = grant.seq
            self.db.update_message(msg)
        ep.applied = checkpoint
        if ep.pending_target is not None and checkpoint >= ep.pending_target:
            ep.pending_target = None
        ep.last_used = at
        self.db.upsert_endpoint(ep)
        retired = self._retire_if_drained(run, name)
        # 处理排队中的推进请求
        pump = self._pump(run)
        return {"endpoint": name, "grant": grant.seq,
                "checkpoint": checkpoint, "applied_events": events,
                "retired_edges": retired, "pump": pump}

    def _retire_if_drained(self, run, receiver):
        """retiring 边的保管消息全部应用后才可真正退役。"""
        retired = []
        for edge in self.inbound[(run, receiver)]:
            if edge.state != EDGE_RETIRING:
                continue
            outstanding = [
                m for m in self.held[(run, receiver)]
                if m.sender == edge.sender
                and m.state in (MSG_PENDING, MSG_BUFFERED, MSG_DELIVERED)
            ]
            if not outstanding:
                edge.state = EDGE_RETIRED
                edge.retired_at = edge.retired_at
                self.db.upsert_edge(edge)
                retired.append(f"{edge.sender}->{edge.receiver}")
        return retired

    # --------------------------------------------------------------- restart
    def _restart(self, at, record):
        # 中间件重启：状态已在同一持久库内，重建内存视图即可继续
        self.endpoints, self.edges, self.messages, self.grants = \
            self.db.load_all()
        self.inbound = defaultdict(list)
        self.outbound = defaultdict(list)
        self.held = defaultdict(list)
        for edge in self.edges.values():
            self._index_edge(edge)
        for msg in self.messages.values():
            if msg.state in (MSG_PENDING, MSG_BUFFERED, MSG_DELIVERED):
                self.held[(msg.run, msg.receiver)].append(msg)
        for key in self.held:
            self.held[key].sort(key=lambda m: (m.tick, m.seq))
        return {"restarted": True, "at": at,
                "endpoints": len(self.endpoints), "grants": len(self.grants),
                "messages": len(self.messages)}

    # 运维通道：模拟端点意外退出（不属于演练资料的 8 类消息）
    def mark_exited(self, run, name):
        with self.lock:
            self.db.conn.execute("BEGIN IMMEDIATE")
            try:
                ep = self._endpoint(run, name)
                ep.status = ENDPOINT_EXITED
                ep.request_target = None
                ep.request_ref = None
                self.db.upsert_endpoint(ep)
                self.db.append_log(-1, "exit",
                                   {"run": run, "endpoint": name},
                                   {"exited": name})
                pump = self._pump(run)
                self.db.conn.commit()
            except Exception:
                self.db.conn.rollback()
                raise
        return {"exited": name, "pump": pump}

    # ------------------------------------------------------------------ pump
    def _pump(self, run):
        """重新评估所有端点的安全推进，持续颁发直到无人可推进。"""
        granted_summary = []
        progressed = True
        while progressed:
            progressed = False
            self._admit_pending(run)
            for key in sorted(k for k in self.endpoints if k[0] == run):
                ep = self.endpoints[key]
                grant = self._try_grant(ep)
                if grant is not None:
                    granted_summary.append(self._grant_view(grant))
                    progressed = True
        waiting = self._waiting_map(run)
        return {"granted": granted_summary, "waiting": waiting}

    def _admit_pending(self, run):
        """joining 边满足承诺条件后激活，并把暂存消息转为正式保管。"""
        for edge in list(self.edges.values()):
            if edge.run != run or edge.state != EDGE_JOINING:
                continue
            if edge.promise_lower is None or edge.promise_boot != \
                    self._endpoint(run, edge.sender).boot:
                continue
            if edge.promise_lower > edge.boundary:
                edge.state = EDGE_ACTIVE
                self.db.upsert_edge(edge)
                for msg in self.held[(run, edge.receiver)]:
                    if msg.sender == edge.sender and msg.state == MSG_PENDING:
                        # lower 是排他下界，tick >= lower 即合法；
                        # 矛盾消息在事件受理时已被拦截，这里统一转为保管
                        msg.state = MSG_BUFFERED
                        self.db.update_message(msg)
                self.held[(run, edge.receiver)].sort(
                    key=lambda m: (m.tick, m.seq))

    def _try_grant(self, ep: Endpoint):
        if ep.status != ENDPOINT_LIVE or ep.has_open_grant:
            return None
        if ep.request_target is None or ep.request_target <= ep.applied:
            return None
        cap_blocked, cap = self._safe_cap(ep)
        if cap_blocked or cap is not None and cap <= ep.applied:
            return None
        tick = ep.request_target if cap is None else min(ep.request_target, cap)
        # 交付 tick 时刻的全部已确定事件（同一逻辑时刻批量交付）
        batch = [
            m for m in self.held[(ep.run, ep.name)]
            if m.state == MSG_BUFFERED and m.tick <= tick
        ]
        # 安全上限保证不会有更晚的消息被漏在 grant 之外：
        # buffered 且 tick <= tick 的全部随单交付
        grant = Grant(seq=self._next_seq("grant"), run=ep.run,
                      endpoint=ep.name, tick=tick, boot=ep.boot,
                      request_ref=ep.request_ref or "")
        for msg in sorted(batch, key=lambda m: (m.tick, m.seq)):
            msg.state = MSG_DELIVERED
            msg.grant_seq = grant.seq
            grant.messages.append(msg.seq)
            self.db.update_message(msg)
        ep.granted = tick
        ep.request_target = None
        ep.request_ref = None
        if ep.pending_target is not None and tick >= ep.pending_target:
            ep.pending_target = None
        self.db.upsert_endpoint(ep)
        # 接收方继续前进时，待入场边的边界同步抬升：
        # 发送端必须承诺覆盖接收方最新的授权位置，迟到消息才不会穿越
        for edge in self.inbound[(ep.run, ep.name)]:
            if edge.state == EDGE_JOINING and edge.boundary < tick:
                edge.boundary = tick
                self.db.upsert_edge(edge)
        self.db.insert_grant(grant)
        self.grants[grant.seq] = grant
        return grant

    def _safe_cap(self, ep: Endpoint):
        """返回 (硬阻塞, 可安全授予的最大逻辑时刻)。

        硬阻塞：存在 active/retiring 边，但其发送端已退出或没有当前 boot
        的承诺，此时接收方必须等待。无任何生效边时 cap 为 None，代表无界。
        """
        cap = None
        blocked = False
        for edge in self.inbound[(ep.run, ep.name)]:
            if edge.state == EDGE_RETIRED:
                continue
            sender = self.endpoints[(ep.run, edge.sender)]
            if edge.state == EDGE_JOINING:
                # 依赖已声明但尚未入场：不能假定没有 <= 当前位置的在途消息，
                # 必须等发送端用当前 boot 的承诺越过入场边界
                blocked = True
                continue
            # active / retiring 都仍约束接收方
            if sender.status == ENDPOINT_EXITED:
                blocked = True
                continue
            if edge.promise_lower is None \
                    or edge.promise_boot != sender.boot:
                blocked = True
                continue
            cap = edge.promise_lower if cap is None \
                else min(cap, edge.promise_lower)
        # 最早保管事件的时刻必须成为可授予边界：接收方先在该 tick 收齐
        # 同刻全部已确定事件并应用，之后才有资格越过它
        for msg in self.held[(ep.run, ep.name)]:
            if msg.state == MSG_BUFFERED:
                cap = msg.tick if cap is None else min(cap, msg.tick)
        return blocked, cap

    def _waiting_map(self, run):
        """每个等待中的端点缺什么事实（供查询与死锁判定）。

        等待资格：端点表达过尚未满足的推进意愿（pending_target 仍大于
        已应用位置），且当前没有可立即颁发的授予。
        """
        result = {}
        for key in sorted(k for k in self.endpoints if k[0] == run):
            ep = self.endpoints[key]
            if ep.status != ENDPOINT_LIVE:
                continue
            wants = ep.pending_target
            if wants is None or wants <= ep.applied or ep.has_open_grant:
                continue
            facts = []
            blocked = False
            for edge in self.inbound[(run, ep.name)]:
                if edge.state == EDGE_JOINING:
                    facts.append({
                        "edge": f"{edge.sender}->{ep.name}",
                        "reason": "edge-joining",
                        "boundary": edge.boundary,
                        "need": f"承诺 lower >= {edge.boundary + 1}"})
                    blocked = True
                    continue
                if edge.state == EDGE_RETIRED:
                    continue
                sender = self.endpoints[(run, edge.sender)]
                if sender.status == ENDPOINT_EXITED:
                    blocked = True
                    facts.append({
                        "edge": f"{edge.sender}->{ep.name}",
                        "reason": "sender-exited",
                        "need": f"{edge.sender} 以新 boot 重新加入并重新承诺"})
                    continue
                if edge.promise_lower is None \
                        or edge.promise_boot != sender.boot:
                    facts.append({
                        "edge": f"{edge.sender}->{ep.name}",
                        "reason": "no-current-promise",
                        "need": f"{edge.sender} 的当前 boot 承诺 lower"})
                    continue
                if edge.promise_lower <= ep.applied:
                    facts.append({
                        "edge": f"{edge.sender}->{ep.name}",
                        "reason": "promise-not-advanced",
                        "lower": edge.promise_lower,
                        "need": "更大的承诺排他下界"})
            earliest = next(
                (m for m in self.held[(run, ep.name)]
                 if m.state == MSG_BUFFERED), None)
            if earliest is not None and earliest.tick <= ep.applied:
                facts.append({"reason": "undelivered-event",
                              "tick": earliest.tick,
                              "need": "授予并交付该时刻事件"})
            if facts:
                result[ep.name] = {"target": wants,
                                   "applied": ep.applied, "facts": facts,
                                   "hard_blocked": blocked}
        return result

    # --------------------------------------------------------------- 死锁环
    def find_deadlocks(self, run):
        """在等待图上找强连通环，并区分崩溃硬阻塞与承诺软等待。"""
        waiting = self._waiting_map(run)
        graph = defaultdict(set)
        for name, info in waiting.items():
            for fact in info["facts"]:
                if "edge" in fact:
                    peer = fact["edge"].split("->")[0]
                    if peer in waiting:
                        graph[name].add(peer)
        # Tarjan 强连通分量
        index_counter = [0]
        stack, lowlink, index, on_stack, components = [], {}, {}, set(), []

        def strongconnect(node):
            index[node] = index_counter[0]
            lowlink[node] = index_counter[0]
            index_counter[0] += 1
            stack.append(node)
            on_stack.add(node)
            for nxt in graph[node]:
                if nxt not in index:
                    strongconnect(nxt)
                    lowlink[node] = min(lowlink[node], lowlink[nxt])
                elif nxt in on_stack:
                    lowlink[node] = min(lowlink[node], index[nxt])
            if lowlink[node] == index[node]:
                component = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    component.append(w)
                    if w == node:
                        break
                components.append(component)

        for node in list(graph):
            if node not in index:
                strongconnect(node)
        cycles = []
        for component in components:
            if len(component) > 1 or (
                    len(component) == 1 and component[0] in graph[component[0]]):
                members = sorted(component)
                cycles.append({
                    "members": members,
                    "edges": [f"{a} 等待 {sorted(graph[a])}" for a in members],
                    "missing_facts": {
                        name: waiting[name]["facts"] for name in members},
                    "hard_blocked": {
                        name: waiting[name]["hard_blocked"] for name in members},
                })
        cycle_members = {m for cyc in cycles for m in cyc["members"]}
        unrelated = sorted(
            name for (r, name) in self.endpoints
            if r == run and name not in waiting and name not in cycle_members
            and self.endpoints[(r, name)].status == ENDPOINT_LIVE)
        return {"run": run, "cycles": cycles,
                "unaffected": unrelated}

    # --------------------------------------------------------------- inspect
    def _message_view(self, msg: Message):
        return {"seq": msg.seq, "event": msg.event, "from": msg.sender,
                "to": msg.receiver, "tick": msg.tick, "boot": msg.boot,
                "state": msg.state, "grant_seq": msg.grant_seq}

    def _grant_view(self, grant: Grant):
        return {"seq": grant.seq, "endpoint": grant.endpoint,
                "tick": grant.tick, "boot": grant.boot,
                "request": grant.request_ref, "state": grant.state,
                "checkpoint": grant.checkpoint,
                "events": [self._message_view(self.messages[s])
                           for s in grant.messages]}

    def _inspect(self, at, record):
        run = _require_str(record.get("run"), "run")
        if run not in {r for r, _ in self.endpoints}:
            raise ProtocolError(f"未知 run {run}")
        endpoint = record.get("endpoint")
        if endpoint is not None:
            ep = self._endpoint(run, endpoint)
            return self._endpoint_lens(ep)
        return {"run": run,
                "endpoints": {
                    name: self._endpoint_lens(self.endpoints[(run, name)])
                    for (r, name) in sorted(self.endpoints) if r == run},
                "edges": [self._edge_view(e) for e in sorted(
                    (e for e in self.edges.values() if e.run == run),
                    key=lambda e: (e.sender, e.receiver))],
                "deadlocks": self.find_deadlocks(run)}

    def _endpoint_lens(self, ep: Endpoint):
        grants = [
            self._grant_view(g) for g in sorted(
                (g for g in self.grants.values()
                 if g.run == ep.run and g.endpoint == ep.name),
                key=lambda g: g.seq)]
        causal = [
            self._message_view(m)
            for m in self.held[(ep.run, ep.name)]
            if m.state in (MSG_PENDING, MSG_BUFFERED, MSG_DELIVERED)
        ]
        applied_events = [
            self._message_view(m)
            for m in self.messages.values()
            if m.run == ep.run and m.receiver == ep.name
            and m.state == "applied"]
        waiting = self._waiting_map(ep.run).get(ep.name)
        return {
            "endpoint": ep.name, "status": ep.status, "boot": ep.boot,
            "applied": ep.applied, "granted": ep.granted,
            "request_target": ep.request_target,
            "pending_target": ep.pending_target,
            "open_grant": next(
                (self._grant_view(g) for g in self.grants.values()
                 if g.run == ep.run and g.endpoint == ep.name
                 and g.state == GRANT_OPEN), None),
            "grants": grants,
            "causal_messages": causal,
            "applied_events": applied_events,
            "waiting": waiting,
            "admission": [
                self._edge_view(e) for e in self.inbound[(ep.run, ep.name)]
                if e.state == EDGE_JOINING],
        }

    def _edge_view(self, edge: Edge):
        return {"edge": f"{edge.sender}->{edge.receiver}",
                "state": edge.state, "boundary": edge.boundary,
                "promise_lower": edge.promise_lower,
                "promise_boot": edge.promise_boot,
                "retired_at": edge.retired_at}
