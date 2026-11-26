"""本地 HTTP 服务：只监听 127.0.0.1，供合成仿真器进程对接。

接口
----
POST /command   8 类演练消息，body 即 record；服务自动维护到达秒点
POST /exit      运维通道，模拟端点意外退出
GET  /inspect?run=&endpoint=
GET  /deadlocks?run=
GET  /log       追加日志（同一运行口径的审计转录）
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import Coordinator, ProtocolError
from .storage import Storage


class _Handler(BaseHTTPRequestHandler):
    coordinator: Coordinator = None
    clock_lock = threading.Lock()
    clock = 0

    def log_message(self, fmt, *args):  # 安静运行
        return

    def _send(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path == "/inspect":
                run = query.get("run", [""])[0]
                record = {"run": run}
                if query.get("endpoint"):
                    record["endpoint"] = query["endpoint"][0]
                self._send(200, self.coordinator.process(
                    self._tick(), "inspect", record))
            elif parsed.path == "/deadlocks":
                run = query.get("run", [""])[0]
                self._send(200, self.coordinator.find_deadlocks(run))
            elif parsed.path == "/log":
                self._send(200, {"log": self.coordinator.db.read_log()})
            elif parsed.path == "/health":
                self._send(200, {"ok": True})
            else:
                self._send(404, {"error": "未知路径"})
        except (ProtocolError, ValueError, KeyError) as error:
            self._send(400, {"error": str(error)})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        try:
            record = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as error:
            self._send(400, {"error": f"JSON 解析失败：{error}"})
            return
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/command":
                kind = record.pop("kind", None)
                if kind is None:
                    raise ProtocolError("body 需要 kind 字段")
                result = self.coordinator.process(
                    self._tick(), kind, record)
                self._send(200, result)
            elif parsed.path == "/exit":
                result = self.coordinator.mark_exited(
                    record["run"], record["endpoint"])
                self._send(200, result)
            else:
                self._send(404, {"error": "未知路径"})
        except (ProtocolError, ValueError, KeyError) as error:
            self._send(400, {"error": str(error)})

    @classmethod
    def _tick(cls):
        with cls.clock_lock:
            cls.clock += 1
            return cls.clock


def serve(db_path, host="127.0.0.1", port=8765):
    storage = Storage(db_path)
    coordinator = Coordinator(storage)
    handler = type("BoundHandler", (_Handler,), {"coordinator": coordinator})
    httpd = ThreadingHTTPServer((host, port), handler)
    return httpd, coordinator
