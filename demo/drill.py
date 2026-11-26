"""训练场联调演示：启动协调服务，三个合成仿真器完整走一轮。

演示内容
--------
1. 关节/视觉/场景三个仿真端加入，声明影响拓扑；
2. 承诺与事件按因果顺序交付，同刻事件批量授予；
3. 互不依赖的记录端全速推进；
4. 关节端崩溃 -> 依赖方硬等待 -> 新代次重启重承诺后恢复；
5. 查询统一口径的授予、应用、等待视图与审计日志。

运行：python3 -m demo.drill
"""

from __future__ import annotations

import json
import tempfile
import threading
from pathlib import Path

from fedtime.server import serve
from fedtime.simulator import CoordinatorClient, SyntheticSimulator


def show(title, payload):
    print(f"\n== {title} ==")
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def main():
    tmp = tempfile.TemporaryDirectory()
    httpd, _ = serve(Path(tmp.name) / "drill.db", port=0)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    client = CoordinatorClient(f"http://127.0.0.1:{port}")
    run = "drill"

    joint = SyntheticSimulator(client, run, "joint")
    vision = SyntheticSimulator(client, run, "vision")
    scene = SyntheticSimulator(client, run, "scene")
    logger = SyntheticSimulator(client, run, "logger")
    for sim in (joint, vision, scene, logger):
        sim.join()

    # 拓扑：关节影响视觉与场景；视觉影响场景
    for sender, receiver in (("joint", "vision"), ("joint", "scene"),
                             ("vision", "scene")):
        client.command("edge", run=run, **{"from": sender, "to": receiver},
                       active=True)

    # 承诺 + 同刻两条事件
    joint.promise("vision", 5)
    joint.promise("scene", 6)
    vision.promise("scene", 7)
    joint.send("angle-a", "vision", 5)
    joint.send("angle-b", "vision", 5)
    grant = vision.take_grant(vision.advance(20, "v-1"))
    show("视觉端在边界 tick=5 批量收齐同刻事件", grant)
    vision.confirm()

    # 无依赖的记录端全速推进
    solo = logger.take_grant(logger.advance(100, "log-1"))
    show("无依赖的记录端直接授予到目标时刻", solo)
    logger.confirm()

    # 关节端崩溃：场景端进入硬等待
    joint.crash()
    blocked = scene.advance(20, "s-1")
    show("发送端崩溃后，场景端硬等待", blocked["waiting"])

    # 新代次重启，重新承诺后恢复（视觉端承诺仍然有效）
    joint.restart(new_boot=2)
    promised = joint.promise("scene", 8)
    show("新代次承诺到达，等待请求在同一泵内兑现",
         promised["pump"]["granted"])

    show("死锁检测（当前无环）", client.deadlocks(run))
    show("场景端统一查询视图", client.inspect(run, "scene"))
    print(f"\n审计日志共 {len(client._get('/log')['log'])} 条，"
          "持久库位于临时目录，进程重启后可按逻辑时刻接续。")

    httpd.shutdown()
    httpd.server_close()
    tmp.cleanup()


if __name__ == "__main__":
    main()
