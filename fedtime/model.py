"""领域模型与状态常量。

逻辑时间为非负整数。承诺 ``lower`` 为排他下界：发送端承诺此后新消息的
tick 不会小于 ``lower``，即 ``tick >= lower`` 才可能出现。
"""

from __future__ import annotations

from dataclasses import dataclass, field

# 端点生命周期
ENDPOINT_LIVE = "live"
ENDPOINT_EXITED = "exited"

# 依赖边生命周期
EDGE_JOINING = "joining"      # 等待发送端达到入场边界
EDGE_ACTIVE = "active"        # 已入场，参与下界计算与消息受理
EDGE_RETIRING = "retiring"    # 停用申请已受理，等待保管消息处理完
EDGE_RETIRED = "retired"      # 已退役，不再影响接收方

# 消息保管状态
MSG_PENDING = "pending"       # 入场前暂存，尚未达到承诺入场条件
MSG_BUFFERED = "buffered"     # 已入场保管，等待随授予交付
MSG_DELIVERED = "delivered"   # 已随授予交付，等待 applied 确认

# 授予状态
GRANT_OPEN = "open"           # 已授予，等待端点应用确认
GRANT_SETTLED = "settled"     # 端点已确认应用


def require_int(value, name):
    """逻辑时间只接受非负整数；bool 是 int 子类，明确拒绝。"""
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} 必须是非负整数")
    return value


@dataclass
class Endpoint:
    run: str
    name: str
    boot: int
    status: str = ENDPOINT_LIVE
    applied: int = 0
    granted: int = 0
    request_target: int | None = None
    request_ref: str | None = None
    # 最近一次尚未完全满足的推进意愿：被开放授予 fence 后仍保留，
    # 供等待图与死锁判定使用
    pending_target: int | None = None
    last_used: int = 0

    @property
    def has_open_grant(self) -> bool:
        return self.granted > self.applied


@dataclass
class Edge:
    run: str
    sender: str
    receiver: str
    state: str = EDGE_JOINING
    # 入场边界：发送端须承诺 lower > boundary（即不再有 <= boundary 的消息）
    boundary: int = 0
    # 当前 boot 下已受理的最高承诺排他下界
    promise_lower: int | None = None
    promise_boot: int | None = None
    retired_at: int | None = None


@dataclass
class Message:
    seq: int
    run: str
    event: str
    sender: str
    receiver: str
    tick: int
    boot: int
    state: str = MSG_BUFFERED
    grant_seq: int | None = None
    delivered_order: int | None = None


@dataclass
class Grant:
    seq: int
    run: str
    endpoint: str
    tick: int
    boot: int
    request_ref: str
    state: str = GRANT_OPEN
    checkpoint: int | None = None
    # 随本次授予交付的消息 seq（按因果顺序）
    messages: list[int] = field(default_factory=list)
