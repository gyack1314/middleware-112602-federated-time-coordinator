"""持久化与进程重启接续测试。"""

import tempfile
import unittest
from pathlib import Path

from fedtime.engine import Coordinator
from fedtime.storage import Storage


def build_history(path):
    c = Coordinator(Storage(path))
    c.process(0, "join", {"run": "r", "endpoint": "a", "boot": 1,
                          "applied": 0})
    c.process(0, "join", {"run": "r", "endpoint": "b", "boot": 1,
                          "applied": 0})
    c.process(1, "edge", {"run": "r", "from": "a", "to": "b",
                          "active": True})
    c.process(3, "event", {"run": "r", "event": "kick", "from": "a",
                           "to": "b", "boot": 1, "tick": 6})
    c.process(2, "promise", {"run": "r", "from": "a", "to": "b",
                             "boot": 1, "lower": 9})
    c.process(4, "advance", {"run": "r", "endpoint": "b", "target": 20,
                             "request": "q1"})
    c.process(4, "advance", {"run": "r", "endpoint": "a", "target": 20,
                             "request": "qa"})
    return c


class DurabilityTests(unittest.TestCase):
    def test_state_rebuilds_across_process_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "fed.db"
            build_history(db)
            # 重新打开：模拟进程退出后新进程从同一持久库接续
            reopened = Coordinator(Storage(db))
            ep_b = reopened.endpoints[("r", "b")]
            self.assertEqual((ep_b.applied, ep_b.granted), (0, 6))
            open_grant = next(
                g for g in reopened.grants.values()
                if g.endpoint == "b" and g.state == "open")
            self.assertEqual(open_grant.tick, 6)
            self.assertEqual(
                [m.event for m in reopened.messages.values()
                 if m.grant_seq == open_grant.seq], ["kick"])
            # 接续后可以正常确认并继续推进，逻辑时刻不丢不重
            ack = reopened.process(5, "applied", {
                "run": "r", "endpoint": "b", "boot": 1,
                "grant": open_grant.seq, "checkpoint": 6,
                "events": open_grant.messages})
            self.assertEqual(ack["checkpoint"], 6)
            again = reopened.process(6, "advance", {
                "run": "r", "endpoint": "b", "target": 20,
                "request": "q2"})
            self.assertEqual(again["pump"]["granted"][0]["tick"], 9)

    def test_audit_log_keeps_single_ordered_narrative(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "fed.db"
            build_history(db)
            reopened = Coordinator(Storage(db))
            log = reopened.db.read_log()
            kinds = [row["kind"] for row in log]
            self.assertEqual(kinds, [
                "join", "join", "edge", "event", "promise",
                "advance", "advance"])
            # 日志条目与结果成对持久化
            self.assertTrue(all("result" in row for row in log))

    def test_restart_message_rebuilds_in_memory_indexes(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "fed.db"
            c = build_history(db)
            c.process(0, "restart", {})
            # 重启后索引仍可用：待处理事件与等待视图一致
            lens = c.process(7, "inspect", {"run": "r", "endpoint": "b"})
            self.assertEqual(len(lens["causal_messages"]), 1)
            self.assertEqual(lens["open_grant"]["tick"], 6)


if __name__ == "__main__":
    unittest.main()
