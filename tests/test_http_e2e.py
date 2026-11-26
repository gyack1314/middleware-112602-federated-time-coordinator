"""HTTP 服务与合成仿真器的端到端测试。"""

import tempfile
import threading
import unittest
from pathlib import Path

from fedtime.server import serve
from fedtime.simulator import CoordinatorClient, SyntheticSimulator


class HttpEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        db = Path(self.tmp.name) / "http.db"
        self.httpd, _ = serve(db, port=0)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.client = CoordinatorClient(
            f"http://127.0.0.1:{self.port}")

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=3)
        self.tmp.cleanup()

    def test_full_federation_lifecycle(self):
        axis = SyntheticSimulator(self.client, "run1", "axis")
        view = SyntheticSimulator(self.client, "run1", "view")
        loner = SyntheticSimulator(self.client, "run1", "loner")
        for sim in (axis, view, loner):
            sim.join()
        self.client.command(
            "edge", run="run1", **{"from": "axis", "to": "view"},
            active=True)
        axis.send("frame", "view", 5)
        axis.promise("view", 7)
        # 无依赖的 loner 全速推进（独立组件不受 axis/view 影响）
        loner_grant = loner.advance(100, "solo")
        self.assertEqual(
            loner_grant["pump"]["granted"][0]["endpoint"], "loner")
        self.assertEqual(loner_grant["pump"]["granted"][0]["tick"], 100)
        loner.take_grant(loner_grant)
        loner.confirm()
        # view 在边界 5 收到同刻事件
        view_result = view.advance(50, "v1")
        grant = view.take_grant(view_result)
        self.assertEqual(grant["tick"], 5)
        self.assertEqual([m["event"] for m in grant["events"]], ["frame"])
        view.confirm()
        # axis 崩溃：view 无法继续获得承诺，进入硬等待
        axis.crash()
        blocked = view.advance(50, "v2")
        self.assertEqual(blocked["pump"]["granted"], [])
        self.assertTrue(blocked["waiting"]["hard_blocked"])
        # 新代次重启并重新承诺：等待中的请求在承诺泵里自动兑现
        axis.restart(new_boot=2)
        promised = axis.promise("view", 12)
        self.assertEqual(promised["state"], "active")
        self.assertEqual(promised["pump"]["granted"][0]["tick"], 12)
        # 之后的重复推进请求被开放授予 fence
        recovered = view.advance(50, "v3")
        self.assertEqual(recovered["pump"]["granted"], [])
        self.assertIn("queued", recovered)

    def test_inspect_and_deadlock_endpoints(self):
        a = SyntheticSimulator(self.client, "run2", "a")
        b = SyntheticSimulator(self.client, "run2", "b")
        a.join()
        b.join()
        self.client.command(
            "edge", run="run2", **{"from": "a", "to": "b"}, active=True)
        self.client.command(
            "edge", run="run2", **{"from": "b", "to": "a"}, active=True)
        a.advance(10, "a-go")
        b.advance(10, "b-go")
        report = self.client.deadlocks("run2")
        self.assertEqual(len(report["cycles"]), 1)
        self.assertEqual(report["cycles"][0]["members"], ["a", "b"])
        lens = self.client.inspect("run2", "a")
        self.assertEqual(lens["waiting"]["facts"][0]["reason"],
                         "edge-joining")

    def test_health(self):
        self.assertEqual(self.client._get("/health"), {"ok": True})


if __name__ == "__main__":
    unittest.main()
