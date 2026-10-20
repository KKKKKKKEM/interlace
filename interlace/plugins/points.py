"""核心钩子点及其值类型：节点调用、采集、生命周期事实，以及 Runtime 边界的事件发布、Work 路由与交付和执行外层。

Engine 层的点定义在 `interlace.engine.points`，本模块重导出；Runtime 边界的点只依赖 engine 与 spi，
定义在本模块，供 Router、Worker 与插件共同引用，避免插件层反向依赖 runtime。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from ..engine.events import Event
from ..engine.extension import WrapPoint
from ..engine.hooks import HookPhase, NodeCall, NodeInspection, Outputs, StopGraph
from ..engine.observation import RuntimeEvent, RuntimeEventKind
from ..engine.points import NODE_CALL, NODE_INSPECT, RUNTIME_EVENT
from ..spi.runtime import Delivery, DeliveryResult, Work

if TYPE_CHECKING:
    from ..engine.execution import Execution
    from ..engine.graph import ExecutionPlan, Graph
    from ..engine.slots import Slot


@dataclass(frozen=True, slots=True)
class ExecutionCall:
    """一次 Graph execution 的外层调用参数，EXECUTION_RUN 中间件可替换 inputs 与 options。

    Attributes:
        execution: 已登记、尚未开始的执行句柄。
        graph: 冻结 Graph。
        inputs: 入口输入。
        options: 本次执行配置的只读快照。
        plan: 限定执行范围的计划；None 执行完整 Graph。
        slot: 当前逻辑执行链的 Slot；直接执行时为 None。
        work: 触发本次执行的 Work；直接执行时为 None。
    """

    execution: Execution
    graph: Graph
    inputs: Any
    options: Mapping[str, Any] = field(default_factory=dict)
    plan: ExecutionPlan | None = None
    slot: Slot | None = None
    work: Work | None = None

    def __post_init__(self) -> None:
        """把 options 冻结为只读映射。"""

        object.__setattr__(self, "options", MappingProxyType(dict(self.options)))


EVENT_EMIT: WrapPoint[Event, Event] = WrapPoint(
    "interlace.event/emit", scope=("event_type",), value_type=Event
)
"""包裹 Router 对一个 Event 的分发：可改写或替换 Event、短路（返回 Event 但不分发）、观察分发结果；
抛出异常时 emit 以 EventDispatchError 失败。中间件返回的 Event 作为 emit() 的结果。"""

WORK_ROUTE: WrapPoint[Work, None] = WrapPoint(
    "interlace.work/route", scope=("event_type", "graph", "queue"), value_type=Work
)
"""包裹路由把 Event 转为 Work 后的提交：可设置 id、options 或 limits，短路即不提交，抛出异常即拒绝。"""

WORK_DELIVER: WrapPoint[Delivery, DeliveryResult] = WrapPoint(
    "interlace.work/deliver", scope=("queue", "graph")
)
"""包裹 Worker 对一次交付的处理，可短路、改写结果（含 RETRY）、计时或限流。"""

EXECUTION_RUN: WrapPoint[ExecutionCall, None] = WrapPoint(
    "interlace.execution/run", scope=("graph",)
)
"""包裹直接执行与队列执行的公共外层；中间件只能替换 inputs 与 options，其余字段保持身份不变。"""

__all__ = [
    "EVENT_EMIT",
    "EXECUTION_RUN",
    "NODE_CALL",
    "NODE_INSPECT",
    "RUNTIME_EVENT",
    "WORK_DELIVER",
    "WORK_ROUTE",
    "ExecutionCall",
    "HookPhase",
    "NodeCall",
    "NodeInspection",
    "Outputs",
    "RuntimeEvent",
    "RuntimeEventKind",
    "StopGraph",
]
