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

- event-before-time-grant.jsonl
- dependency-joins-late.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
