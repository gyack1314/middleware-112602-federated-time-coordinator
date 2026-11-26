"""协调引擎的单元语义测试。"""

import unittest

from fedtime.engine import Coordinator, ProtocolError
from fedtime.model import (
    EDGE_ACTIVE,
    EDGE_JOINING,
    MSG_BUFFERED,
)
from fedtime.storage import Storage


def make():
    return Coordinator(Storage(":memory:"))


def join(c, name, boot=1, applied=0, run="r"):
    return c.process(0, "join", {"run": run, "endpoint": name,
                                 "boot": boot, "applied": applied})


def edge(c, a, b, active=True, run="r"):
    return c.process(1, "edge", {"run": run, "from": a, "to": b,
                                 "active": active})


def promise(c, a, b, lower, boot=1, run="r"):
    return c.process(2, "promise", {"run": run, "from": a, "to": b,
                                    "boot": boot, "lower": lower})


def event(c, a, b, tick, name="e", boot=1, run="r"):
    return c.process(3, "event", {"run": run, "event": name, "from": a,
                                  "to": b, "boot": boot, "tick": tick})


def advance(c, name, target, request="req", run="r"):
    return c.process(4, "advance", {"run": run, "endpoint": name,
                                    "target": target, "request": request})


def applied(c, name, grant, checkpoint, events, boot=1, run="r"):
    return c.process(5, "applied", {"run": run, "endpoint": name,
                                    "boot": boot, "grant": grant,
                                    "checkpoint": checkpoint,
                                    "events": events})


def self_refresh(c, grant_seq):
    return c.grants[grant_seq].state


class PromiseAndBoundTests(unittest.TestCase):
    def test_lower_is_exclusive_grant_stops_before_it(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        promise(c, "a", "b", 5)       # 新消息 tick >= 5
        result = advance(c, "b", 10)
        self.assertEqual(result["pump"]["granted"][0]["tick"], 5)

    def test_grant_waits_when_no_promise(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        result = advance(c, "b", 10)
        self.assertEqual(result["pump"]["granted"], [])
        self.assertEqual(result["waiting"]["facts"][0]["reason"],
                         "edge-joining")

    def test_promise_is_monotonic_per_boot(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        promise(c, "a", "b", 5)
        with self.assertRaisesRegex(ProtocolError, "单调"):
            promise(c, "a", "b", 4)

    def test_event_below_own_promise_is_contract_violation(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        promise(c, "a", "b", 5)
        with self.assertRaisesRegex(ProtocolError, "承诺"):
            event(c, "a", "b", 4)

    def test_minimum_promise_across_edges_caps_grant(self):
        c = make()
        for name in ("a", "x", "b"):
            join(c, name)
        edge(c, "a", "b")
        edge(c, "x", "b")
        promise(c, "a", "b", 9)
        promise(c, "x", "b", 6)
        result = advance(c, "b", 20)
        self.assertEqual(result["pump"]["granted"][0]["tick"], 6)


class SameTickTests(unittest.TestCase):
    def test_all_events_at_same_tick_deliver_together_in_order(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        event(c, "a", "b", 7, name="first")
        event(c, "a", "b", 7, name="second")
        promise(c, "a", "b", 8)
        result = advance(c, "b", 10)
        grant = result["pump"]["granted"][0]
        self.assertEqual(grant["tick"], 7)
        self.assertEqual([m["event"] for m in grant["events"]],
                         ["first", "second"])

    def test_cannot_cross_tick_until_batch_applied(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        event(c, "a", "b", 7)
        promise(c, "a", "b", 10)
        first = advance(c, "b", 10)["pump"]["granted"][0]
        # 授予只到 7；应用前再次申请仍被 fence 挡住，请求排队登记
        queued = advance(c, "b", 10, request="second")
        self.assertEqual(queued["pump"]["granted"], [])
        # 应用同刻事件后，排队的请求立刻在泵里兑现到承诺边界 10
        settled = applied(c, "b", first["seq"], 7,
                          [m["seq"] for m in first["events"]])
        second = settled["pump"]["granted"][0]
        self.assertEqual(second["tick"], 10)


class FencingTests(unittest.TestCase):
    def test_stale_boot_cannot_confirm_grant(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        promise(c, "a", "b", 5)
        grant = advance(c, "b", 9)["pump"]["granted"][0]
        with self.assertRaisesRegex(ProtocolError, "代次"):
            c.process(5, "applied", {"run": "r", "endpoint": "b",
                                     "boot": 99, "grant": grant["seq"],
                                     "checkpoint": 5, "events": []})

    def test_same_grant_cannot_be_confirmed_twice(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        promise(c, "a", "b", 5)
        grant = advance(c, "b", 5)["pump"]["granted"][0]
        applied(c, "b", grant["seq"], 5, [])
        with self.assertRaisesRegex(ProtocolError, "重复"):
            applied(c, "b", grant["seq"], 5, [])

    def test_new_generation_with_smaller_progress_is_rejected(self):
        c = make()
        join(c, "b", applied=0)
        g = advance(c, "b", 8)["pump"]["granted"][0]
        # 新代次报回超过已授予位置的进度：服务没有授予过该时刻，拒绝
        with self.assertRaisesRegex(ProtocolError, "越过了已授予位置"):
            join(c, "b", boot=2, applied=9)
        # 正确接棒：从已应用位置 0 接，未应用授予按新 boot 重签同一时刻
        joined = join(c, "b", boot=2, applied=0)
        reissued = joined["reissued_grant"]
        self.assertIsNotNone(reissued)
        self.assertEqual(reissued["tick"], g["tick"])
        self.assertEqual(reissued["boot"], 2)
        self.assertNotEqual(reissued["seq"], g["seq"])
        # 旧授予已作废，旧进程无法再执行
        self.assertEqual(self_refresh(c, g["seq"]), "superseded")
        # 应用后再换代，小于已持久化 applied 的位置同样被拒
        c.process(5, "applied", {"run": "r", "endpoint": "b", "boot": 2,
                                 "grant": reissued["seq"], "checkpoint": 8,
                                 "events": []})
        with self.assertRaisesRegex(ProtocolError, "回退"):
            join(c, "b", boot=3, applied=5)

    def test_events_set_must_match_grant_delivery(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        event(c, "a", "b", 4)
        promise(c, "a", "b", 5)
        grant = advance(c, "b", 5)["pump"]["granted"][0]
        with self.assertRaisesRegex(ProtocolError, "事件集合"):
            applied(c, "b", grant["seq"], 4, [])


class AdmissionTests(unittest.TestCase):
    def test_joining_edge_buffers_event_and_activates_on_promise(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        sent = event(c, "a", "b", 9)
        self.assertEqual(sent["state"], "pending")
        # 边界为 0：承诺 lower=1 已可入场，但 tick=9 消息仍先于承诺到达而暂存
        promise(c, "a", "b", 1)
        self.assertEqual(c.edges[("r", "a", "b")].state, EDGE_ACTIVE)
        self.assertEqual(c.messages[sent["event_seq"]].state, MSG_BUFFERED)

    def test_boundary_tracks_receiver_progress(self):
        c = make()
        join(c, "a")
        join(c, "b")
        # 先声明依赖并完成入场，b 才能推进；随后新依赖验证边界跟随
        edge(c, "a", "b")
        promise(c, "a", "b", 13)
        advance(c, "b", 12)
        join(c, "x")
        added = edge(c, "x", "b")
        self.assertEqual(added["boundary"], 12)
        # 只承诺到 10 不能入场，必须覆盖 12
        promise(c, "x", "b", 10)
        self.assertEqual(c.edges[("r", "x", "b")].state, EDGE_JOINING)
        promise(c, "x", "b", 13)
        self.assertEqual(c.edges[("r", "x", "b")].state, EDGE_ACTIVE)

    def test_reactivated_edge_does_not_assume_caught_up_position(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        promise(c, "a", "b", 10)
        # 申请退役：无保管消息，授予并应用到承诺边界后边彻底退役
        edge(c, "a", "b", active=False)
        g = advance(c, "b", 20)["pump"]["granted"][0]
        self.assertEqual(g["tick"], 10)
        retired = applied(c, "b", g["seq"], 10, [])
        self.assertEqual(retired["retired_edges"], ["a->b"])
        g2 = advance(c, "b", 20, request="go")["pump"]["granted"][0]
        applied(c, "b", g2["seq"], 20, [])
        # 重新启用：边界抬到 b 当前授权位置，不能假定 a 已接续
        edge(c, "a", "b", active=True)
        self.assertEqual(c.edges[("r", "a", "b")].state, EDGE_JOINING)
        self.assertEqual(c.edges[("r", "a", "b")].boundary, 20)


class CrashAndRestartTests(unittest.TestCase):
    def test_receivers_wait_on_exited_sender(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        promise(c, "a", "b", 5)
        c.mark_exited("r", "a")
        result = advance(c, "b", 10)
        self.assertEqual(result["pump"]["granted"], [])
        self.assertTrue(result["waiting"]["hard_blocked"])
        self.assertEqual(result["waiting"]["facts"][0]["reason"],
                         "sender-exited")

    def test_rejoin_new_boot_invalidates_old_promise(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        promise(c, "a", "b", 5)
        c.mark_exited("r", "a")
        # 新代次回来但还没重新承诺：b 仍等待
        join(c, "a", boot=2)
        result = advance(c, "b", 10)
        self.assertEqual(result["pump"]["granted"], [])
        joining_edge = c.edges[("r", "a", "b")]
        self.assertEqual(joining_edge.state, EDGE_JOINING)
        # 重新承诺后边激活，等待中的推进请求在同一泵内自动兑现
        promised = promise(c, "a", "b", 8, boot=2)
        self.assertEqual(promised["state"], EDGE_ACTIVE)
        grant = promised["pump"]["granted"][0]
        self.assertEqual(grant["endpoint"], "b")
        self.assertEqual(grant["tick"], 8)

    def test_reconnect_same_boot_redelivers_open_grant(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        event(c, "a", "b", 3)
        promise(c, "a", "b", 5)
        grant = advance(c, "b", 6)["pump"]["granted"][0]
        result = join(c, "b", boot=1)  # 同 boot 重连
        resume = result["resume"]
        self.assertEqual(resume["open_grant"]["seq"], grant["seq"])
        self.assertEqual(len(resume["outstanding_events"]), 1)

    def test_old_boot_rejoin_rejected(self):
        c = make()
        join(c, "a", boot=3)
        with self.assertRaisesRegex(ProtocolError, "过期"):
            join(c, "a", boot=2)


class DeadlockTests(unittest.TestCase):
    def test_cycle_is_reported_with_missing_facts(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        edge(c, "b", "a")
        advance(c, "a", 10, request="a-go")
        advance(c, "b", 10, request="b-go")
        report = c.find_deadlocks("r")
        self.assertEqual(len(report["cycles"]), 1)
        self.assertEqual(report["cycles"][0]["members"], ["a", "b"])
        facts = report["cycles"][0]["missing_facts"]
        self.assertTrue(facts["a"])
        self.assertTrue(facts["b"])

    def test_unrelated_federate_keeps_advancing(self):
        c = make()
        join(c, "a")
        join(c, "b")
        join(c, "loner")
        edge(c, "a", "b")
        edge(c, "b", "a")
        advance(c, "a", 10)
        advance(c, "b", 10)
        result = advance(c, "loner", 10)
        self.assertEqual(result["pump"]["granted"][0]["tick"], 10)
        report = c.find_deadlocks("r")
        self.assertIn("loner", report["unaffected"])

    def test_breaking_cycle_resumes_grants(self):
        c = make()
        join(c, "a")
        join(c, "b")
        edge(c, "a", "b")
        edge(c, "b", "a")
        advance(c, "a", 10)
        advance(c, "b", 10)
        # a 补齐对 b 的承诺：b 的等待请求在同一泵内兑现，环被打破
        promised = promise(c, "a", "b", 5)
        grant = promised["pump"]["granted"][0]
        self.assertEqual(grant["endpoint"], "b")
        self.assertEqual(grant["tick"], 5)
        # 环已不再是强连通（a 仍单方面等待 b 的承诺）
        self.assertEqual(c.find_deadlocks("r")["cycles"], [])


if __name__ == "__main__":
    unittest.main()
