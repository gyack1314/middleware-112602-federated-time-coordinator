"""本地合成仿真器：协调服务的客户端实现与联调演示。

SyntheticSimulator 封装一个仿真参与端的完整生命周期：
加入 -> 承诺 -> 发事件 -> 申请推进 -> 确认应用 -> 崩溃/重启接续。
"""

from __future__ import annotations

import json
import urllib.request


class CoordinatorClient:
    """对本地 HTTP 协调服务的极简 JSON 客户端（仅标准库）。"""

    def __init__(self, base="http://127.0.0.1:8765"):
        self.base = base.rstrip("/")

    def _post(self, path, payload):
        req = urllib.request.Request(
            self.base + path,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())

    def _get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=5) as resp:
            return json.loads(resp.read())

    def command(self, kind, **record):
        body = {"kind": kind, **record}
        return self._post("/command", body)

    def mark_exited(self, run, endpoint):
        return self._post("/exit", {"run": run, "endpoint": endpoint})

    def inspect(self, run, endpoint=None):
        suffix = f"/inspect?run={run}"
        if endpoint:
            suffix += f"&endpoint={endpoint}"
        return self._get(suffix)

    def deadlocks(self, run):
        return self._get(f"/deadlocks?run={run}")


class SyntheticSimulator:
    """一个合成参与端：自动维护 boot 与实际应用位置。"""

    def __init__(self, client: CoordinatorClient, run, name, boot=1,
                 applied=0):
        self.client = client
        self.run = run
        self.name = name
        self.boot = boot
        self.applied = applied
        self.last_grant = None

    def join(self):
        result = self.client.command(
            "join", run=self.run, endpoint=self.name,
            boot=self.boot, applied=self.applied)
        resume = result.get("resume") or {}
        if resume.get("open_grant"):
            self.last_grant = resume["open_grant"]
        return result

    def promise(self, peer, lower):
        return self.client.command(
            "promise", run=self.run, **{"from": self.name, "to": peer},
            boot=self.boot, lower=lower)

    def send(self, event, peer, tick):
        return self.client.command(
            "event", run=self.run, event=event, **{"from": self.name},
            to=peer, boot=self.boot, tick=tick)

    def advance(self, target, request):
        return self.client.command(
            "advance", run=self.run, endpoint=self.name,
            target=target, request=request)

    def confirm(self):
        """确认当前持有的未应用授予，并返回服务回执。"""
        if not self.last_grant or self.last_grant["state"] != "open":
            raise RuntimeError("没有待确认的授予")
        events = [m["seq"] for m in self.last_grant["events"]]
        result = self.client.command(
            "applied", run=self.run, endpoint=self.name, boot=self.boot,
            grant=self.last_grant["seq"],
            checkpoint=self.last_grant["tick"], events=events)
        self.applied = self.last_grant["tick"]
        self.last_grant = None
        return result

    def take_grant(self, advance_result):
        """从推进/泵结果中取出属于本端的授予；拿不到则查询当前开放授予。"""
        for grant in advance_result.get("pump", {}).get("granted", []):
            if grant["endpoint"] == self.name:
                self.last_grant = grant
                return grant
        lens = self.client.inspect(self.run, self.name)
        if lens.get("open_grant"):
            self.last_grant = lens["open_grant"]
            return self.last_grant
        return None

    def crash(self):
        return self.client.mark_exited(self.run, self.name)

    def restart(self, new_boot, applied=None):
        self.boot = new_boot
        if applied is not None:
            self.applied = applied
        result = self.join()
        return result
