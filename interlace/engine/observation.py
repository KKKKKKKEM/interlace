"""只读、不可干预执行结果的 Runtime 生命周期事实，经 RUNTIME_EVENT 钩子点通知。"""

from __future__ import annotations

import enum
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any


class RuntimeEventKind(str, enum.Enum):
    """只读生命周期事件的固定类型。

    Attributes:
        RUNTIME_STARTED: Runtime 完成装配、可以接受工作后发布一次。
        RUNTIME_CLOSING: Runtime 进入 CLOSING、开始排空已接受工作前发布一次。
        RUNTIME_CLOSED: Runtime 排空并关闭全部角色后、执行插件清理前发布一次。
        EXECUTION_STARTED: 执行宿主开始一次 execution。
        EXECUTION_FINISHED: 执行器结束后发布，status 为即将写入的终态；处理函数收到时
            `Execution.status` 可能仍为 RUNNING。
        NODE_STARTED: 一次 Node firing 计步后、调用 Context 工厂前发布。
        NODE_FINISHED: 一次 Node firing 结束后发布，status 为 succeeded 或 failed。
        OUTPUT_ROUTED: 输出通过校验后的端口传播记录，不携带业务值。
        EVENT_PUBLISHED: Router 开始向 EventBus 分发领域事件，先于该事件引起的
            WORK_SUBMITTED；分发失败由 emit 抛出 EventDispatchError，不撤回本记录。
        WORK_SUBMITTED: route 已把事件转换的 Work 交给任务发布端。
        WORK_FINISHED: 一项 Work 交付结束。
    """

    RUNTIME_STARTED = "runtime.started"
    RUNTIME_CLOSING = "runtime.closing"
    RUNTIME_CLOSED = "runtime.closed"
    EXECUTION_STARTED = "execution.started"
    EXECUTION_FINISHED = "execution.finished"
    NODE_STARTED = "node.started"
    NODE_FINISHED = "node.finished"
    OUTPUT_ROUTED = "output.routed"
    EVENT_PUBLISHED = "event.published"
    WORK_SUBMITTED = "work.submitted"
    WORK_FINISHED = "work.finished"


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    """不携带业务输入输出的轻量生命周期事实。

    Attributes:
        kind: 生命周期事件类型。
        graph: 关联 Graph 的注册名称；与 Graph 无关时为 None。
        execution_id: 关联 Execution 的标识；与执行无关时为 None。
        node: 关联的节点 ID；与节点无关时为 None。
        work_id: 关联 Work 的标识；与 Work 无关时为 None。
        event_type: 关联领域事件的类型；与事件无关时为 None。
        status: 结束类事件记录的状态字符串，其他事件为 None。
        error_type: 失败时的异常类型名称，不携带异常对象。
        attributes: 事件附加属性的只读快照。
        occurred_at: 事件创建的 UTC 时间。
    """

    kind: RuntimeEventKind
    graph: str | None = None
    execution_id: str | None = None
    node: str | None = None
    work_id: str | None = None
    event_type: str | None = None
    status: str | None = None
    error_type: str | None = None
    attributes: Mapping[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        """把 attributes 冻结为只读快照。

        Raises:
            TypeError: kind 不是 RuntimeEventKind。
        """

        if not isinstance(self.kind, RuntimeEventKind):
            raise TypeError("runtime event kind must be RuntimeEventKind")
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


# 仅携带观测身份；事件循环提交会复制当前上下文，不携带领域数据。
_NODE_EVENT: ContextVar[RuntimeEvent | None] = ContextVar(
    "interlace.node_event", default=None
)


def current_node_event() -> RuntimeEvent | None:
    """读取当前调用链的节点触发身份，供日志与诊断扩展关联记录。

    Returns:
        当前节点开始事件；不在节点调用链内时为 None。
    """

    return _NODE_EVENT.get()


@contextmanager
def node_observation_scope(event: RuntimeEvent) -> Iterator[None]:
    """在完整 firing 期间关联观测身份，并在清理后恢复上层作用域。

    Args:
        event: 本次节点开始事件。

    Yields:
        同步或异步节点与生成器清理共用的身份作用域。
    """

    token = _NODE_EVENT.set(event)
    try:
        yield
    finally:
        _NODE_EVENT.reset(token)
