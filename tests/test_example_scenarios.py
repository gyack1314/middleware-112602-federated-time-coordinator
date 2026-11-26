"""用既有演练资料验证协调服务的核心语义。"""

import unittest
from pathlib import Path

from fedtime.engine import Coordinator
from fedtime.runner import replay_file
from fedtime.storage import Storage

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


class EventBeforeGrantTests(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator(Storage(":memory:"))
        self.transcript = replay_file(
            EXAMPLES / "event-before-time-grant.jsonl", self.coordinator)

    def test_event_is_held_until_promise_allows_boundary_grant(self):
        # 行序：0-2 join，3 edge，4 event，5 promise(lower=4)，6 advance(scene)
        advance = self.transcript[6]["result"]
        grant = advance["pump"]["granted"][0]
        self.assertEqual(grant["endpoint"], "scene")
        self.assertEqual(grant["tick"], 4)
        self.assertEqual([e["event"] for e in grant["events"]], ["touch"])
        # 完整重放末尾 scene 已换代重启，授权以授予历史为准
        scene_grants = [
            g for g in self.coordinator.grants.values()
            if g.endpoint == "scene"]
        self.assertEqual(scene_grants[0].tick, 4)
        self.assertEqual(scene_grants[0].state, "superseded")

    def test_independent_federate_runs_at_full_speed(self):
        result = self.transcript[7]["result"]  # independent 申请到 8
        grant = result["pump"]["granted"][0]
        self.assertEqual(grant["endpoint"], "independent")
        self.assertEqual(grant["tick"], 8)
        self.assertEqual(grant["events"], [])

    def test_open_grant_fences_second_request_until_applied(self):
        # 9 行（at 7）的第二次申请必须排队，不能颁发第二个授予
        result = self.transcript[9]["result"]
        self.assertEqual(result["pump"]["granted"], [])
        self.assertIn("queued", result)
        # 重启换代前，scene 只有一个未应用授予
        before_restart = [
            g for g in self.coordinator.grants.values()
            if g.endpoint == "scene" and g.boot == 1]
        self.assertEqual(len(before_restart), 1)

    def test_restart_redelivers_unapplied_grant_and_event(self):
        join_result = self.transcript[11]["result"]  # scene boot 2 接棒
        reissued = join_result["reissued_grant"]
        self.assertEqual(reissued["tick"], 4)
        self.assertEqual(reissued["boot"], 2)
        lens = self.transcript[12]["result"]  # inspect scene
        self.assertEqual(lens["boot"], 2)
        self.assertEqual(lens["applied"], 0)
        open_grant = lens["open_grant"]
        self.assertEqual(open_grant["tick"], 4)
        self.assertEqual(open_grant["boot"], 2)
        self.assertEqual([e["seq"] for e in open_grant["events"]], [1])
        self.assertEqual(
            [m["state"] for m in lens["causal_messages"]], ["delivered"])
        # 旧代次授予已作废，旧进程无法重复执行
        self.assertEqual(
            self.coordinator.grants[reissued["seq"]].state, "open")


class LateDependencyTests(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator(Storage(":memory:"))
        self.transcript = replay_file(
            EXAMPLES / "dependency-joins-late.jsonl", self.coordinator)

    def test_view_advances_before_dependency_exists(self):
        grant = self.transcript[1]["result"]["pump"]["granted"][0]
        self.assertEqual(grant["endpoint"], "view")
        self.assertEqual(grant["tick"], 8)

    def test_late_edge_waits_at_boundary_and_does_not_roll_back(self):
        # 部分重放到边加入为止：授权位置必须停在 8，不被新依赖倒退
        partial = Coordinator(Storage(":memory:"))
        from fedtime.runner import MESSAGE_TYPES
        from scenario_reader import iter_messages
        rows = list(iter_messages(
            EXAMPLES / "dependency-joins-late.jsonl", set(MESSAGE_TYPES)))
        for row in rows[:5]:  # 第 5 行正是 edge 加入
            result = partial.process(row["at"], row["kind"], row["record"])
        self.assertEqual(result["state"], "joining")
        self.assertEqual(result["boundary"], 8)
        view = partial.endpoints[("case", "view")]
        self.assertEqual(view.granted, 8)  # 既有授权不被倒退
        # 全量重放中的边加入结果同样停在 joining
        self.assertEqual(self.transcript[4]["result"]["state"], "joining")

    def test_edge_activates_when_promise_clears_boundary(self):
        promise = self.transcript[5]["result"]
        self.assertEqual(promise["state"], "active")

    def test_deactivation_keeps_held_event_blocking(self):
        # 停用后 view 申请到 11，保管的 tick=10 事件仍须随授予交付
        advance = self.transcript[8]["result"]
        grant = advance["pump"]["granted"][0]
        self.assertEqual(grant["tick"], 10)
        self.assertEqual([e["event"] for e in grant["events"]], ["later"])
        edge = self.coordinator.edges[("case", "scene", "view")]
        self.assertEqual(edge.state, "retiring")

    def test_retirement_completes_only_after_event_applied(self):
        # view 已有两个授予：8（已应用）、10（待应用）
        applied = self.coordinator.process(10, "applied", {
            "run": "case", "endpoint": "view", "boot": 1,
            "grant": 2, "checkpoint": 10, "events": [1]})
        self.assertEqual(applied["retired_edges"], ["scene->view"])
        edge = self.coordinator.edges[("case", "scene", "view")]
        self.assertEqual(edge.state, "retired")
        # 边退役后不再阻塞，view 可越过原申请目标继续推进
        again = self.coordinator.process(11, "advance", {
            "run": "case", "endpoint": "view", "target": 20, "request": "go2"})
        self.assertEqual(again["pump"]["granted"][0]["tick"], 20)


if __name__ == "__main__":
    unittest.main()
