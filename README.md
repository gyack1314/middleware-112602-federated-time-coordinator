# 联合训练仿真的逻辑时间授权与局部推进

合成仿真使用整数逻辑时钟。at 是协调器收消息顺序，tick 与 lower 是业务逻辑时间。lower 不包含该时点本身。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **join**　加入 run 上的 endpoint，boot 为进程代次，applied 为端点实际位置。
- **edge**　运行方添加或请求停用 from/to 的一条事件依赖，active 表示申请的状态。 停用既有依赖时先停止新的来源受理，已经实际保管的消息仍归该轮。
- **event**　发送端提交明确的 event、from、boot、to 及逻辑 tick。
- **promise**　from 端承诺发向 to 的新消息 tick 不会小于 lower。 只有当前 boot 的单调承诺可作新授权依据，换代仍保留已经实际交付的历史。
- **advance**　endpoint 请求推进到 target，引用 request。
- **applied**　端点确认已经应用 grant 和所列事件，boot 与 checkpoint 指当前运行身份和实际时间。grant 是同一接收方在本次联调依次取得的授予标识。
- **restart**　模拟中间件重启。
- **inspect**　按 run 或 endpoint 查询。

## 示例

- event-before-time-grant.jsonl　事件先于授权保管，重启代次重签授予
- dependency-joins-late.jsonl　依赖边迟到入场与退役前的消息处理
- mutual-wait-ring.jsonl　相互等待环与不相干组件继续推进
- crash-and-rejoin.jsonl　发送端换代后承诺失效与安全接续

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 联邦时间协调服务（fedtime 包）

`fedtime` 在上述输入契约之上实现保守同步（conservative synchronization）协调器，
只对接本地合成仿真器，不引入任何第三方依赖。

### 核心保证

- **安全时间授予**：授予时刻不越过任何生效依赖边的承诺排他下界；最早保管
  事件的 tick 成为授予边界，接收方先在该 tick 收齐同刻全部已确定事件并应用，
  之后才能越过它。同一逻辑时刻的多条消息在一个授予内批量、按到达顺序交付。
- **局部推进**：没有依赖关系的端点按自身节奏全速推进；等待关系只约束真正
  有入边的端点。
- **入场边界**：新增或重新启用的边先停在 `joining`，边界取接收方当前授权
  位置并随后续授予抬升；发送端用当前 boot 承诺 `lower > boundary` 后才激活，
  既有参与者的授权不会倒退。
- **代次围栏**：授予带 boot 代次与单调序号，旧代次进程无法确认新授予；
  新代次接棒时旧的未应用授予作废（superseded）并按新 boot 重签同一逻辑时刻，
  同一赋权不可能被两个进程执行。已持久化的实际位置不接受更小进度回退。
- **崩溃与退役**：发送端意外退出后依赖方进入硬等待；边退役前必须先停止受理
  新消息，并等已保管消息全部应用，删除关系本身不会让持有者越过待处理事件。
- **死锁报告**：在等待图上计算强连通分量，指出成环端点、相互等待边与各自
  缺失的事实；不在环上的联调组件继续工作。
- **持久化接续**：每条命令与结果写入只追加审计日志，状态在同一事务内物化到
  SQLite（WAL + synchronous=FULL）。进程重开后从同一库重建，按原逻辑时刻接续。

### 模块

| 文件 | 职责 |
| --- | --- |
| `fedtime/model.py` | 端点、依赖边、消息、授予的领域模型与状态常量 |
| `fedtime/storage.py` | SQLite 审计日志与物化状态，含模式迁移 |
| `fedtime/engine.py` | 协调引擎：入场、泵式授予、围栏、退役、死锁检测 |
| `fedtime/runner.py` | 复用 `scenario_reader` 重放 JSONL 演练资料 |
| `fedtime/server.py` | 仅监听 127.0.0.1 的 HTTP 服务 |
| `fedtime/simulator.py` | 合成仿真器客户端 |
| `fedtime/cli.py` | 命令行：serve / replay / inspect / deadlocks / log |
| `demo/drill.py` | 三仿真器联调端到端演示 |

### 常用命令

    # 重放演练资料并打印逐条转录
    python3 -m fedtime.cli replay examples --transcript

    # 持久化到文件后按统一口径查询
    python3 -m fedtime.cli replay examples/event-before-time-grant.jsonl \
        --db ./run.db
    python3 -m fedtime.cli inspect --db ./run.db --run case
    python3 -m fedtime.cli deadlocks --db ./run.db --run case
    python3 -m fedtime.cli log --db ./run.db

    # 启动本地 HTTP 服务（仿真器进程对接）
    python3 -m fedtime.cli serve --db ./run.db --port 8765

    # 端到端联调演示
    python3 -m demo.drill

HTTP 接口：`POST /command`（body 为带 `kind` 的 record）、`POST /exit`
（运维通道，模拟端点意外退出）、`GET /inspect`、`GET /deadlocks`、
`GET /log`、`GET /health`。
