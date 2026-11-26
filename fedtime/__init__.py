"""联邦时间协调服务。

为本地合成仿真器提供保守逻辑时间协调：
按承诺（promise）授予安全推进时间、按因果顺序交付跨参与者事件、
管理依赖边的入场边界、重启代次与待处理事件重发，并检测相互等待环。
"""

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
)
from .engine import Coordinator, ProtocolError

__all__ = ["Coordinator", "ProtocolError"]
