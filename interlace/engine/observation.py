"""只读、不可干预执行结果的 Runtime 观测事件。"""

from __future__ import annotations

import enum
import itertools
import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from types import MappingProxyType
from typing import Any, Protocol

_LOGGER = logging.getLogger(__name__)


class RuntimeEventKind(str, enum.Enum):
    """只读生命周期事件的固定类型。

    Attributes:
        EXECUTION_STARTED: 执行开始事件。
        EXECUTION_FINISHED: 执行终止事件。
        NODE_STARTED: 节点触发开始事件。
        NODE_FINISHED: 节点触发结束事件。
        OUTPUT_ROUTED: 输出通过校验后的端口传播记录，不携带业务值。
        EVENT_PUBLISHED: 领域事件已发布事件。
        WORK_SUBMITTED: 工作已提交事件。
        WORK_FINISHED: 工作交付结束事件。
    """

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
        kind: 生命周期事件的类型。
        graph: 关联的 Graph 定义或注册名称。
        execution_id: 关联 Execution 的标识。
        node: 关联的节点实例或 Graph 内节点 ID。
        work_id: 关联 Work 的标识。
        event_type: 关联领域事件的类型。
        status: 观测到的执行状态。
        error_type: 观测到的异常类型名称，不携带业务异常对象。
        attributes: 生命周期事件的只读附加属性。
        occurred_at: 观测事件创建的 UTC 时间。
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
        """校验构造字段并固定需要保持不变的数据。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not isinstance(self.kind, RuntimeEventKind):
            raise TypeError("runtime event kind must be RuntimeEventKind")
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


class RuntimeObserver(Protocol):
    def __call__(self, event: RuntimeEvent) -> None:
        """观察事件；返回值被忽略。

        Args:
            event: 需要发布、观察或处理的事件。
        """


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


class ObserverHandle:
    """一项生命周期观察注册的卸载句柄。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _hub: 当前观察者注册所属的分发中心。
        _registration_id: 当前句柄独立拥有的观察注册编号。
    """

    __slots__ = ("_hub", "_registration_id")

    def __init__(self, hub: ObservationHub, registration_id: int) -> None:
        """绑定独立观察注册及其所属分发中心，供后续卸载。

        Args:
            hub: 分发只读事件的观察中心。
            registration_id: 当前句柄负责卸载的唯一注册编号。
        """

        self._hub = hub
        self._registration_id = registration_id

    def detach(self) -> None:
        """幂等解除当前注册，不影响相同回调的其他注册。"""

        self._hub._detach(self._registration_id)


class CompositeObserverHandle:
    """统一卸载多个观察者注册的组合句柄。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _handles: 需要统一卸载的观察句柄集合。
    """

    __slots__ = ("_handles",)

    def __init__(self, handles: tuple[ObserverHandle, ...]) -> None:
        """保存需要统一卸载的观察句柄集合。

        Args:
            handles: 需要统一卸载的观察者句柄。
        """

        self._handles = handles

    def detach(self) -> None:
        """解除当前句柄对应的注册关系。"""

        for handle in self._handles:
            handle.detach()


class ObservationHub:
    """线程安全地分发只读事件；观察者失败不会影响业务执行。

    Attributes:
        _observers: 按注册顺序排列的注册编号与运行时观察者快照。
        _counter: 分配独立注册编号的单调递增计数器。
        _lock: 保护当前组件共享状态的进程内互斥锁。
    """

    def __init__(self) -> None:
        """创建线程安全的观察者快照与注册锁。"""

        self._observers: tuple[tuple[int, RuntimeObserver], ...] = ()
        self._counter = itertools.count()
        self._lock = RLock()

    def attach(self, observer: RuntimeObserver) -> ObserverHandle:
        """注册只读观察者并返回独立卸载句柄。

        Args:
            observer: 接收只读生命周期事件的观察者。

        Returns:
            用于卸载本次注册的句柄。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not callable(observer):
            raise TypeError("runtime observer must be callable")
        with self._lock:
            registration_id = next(self._counter)
            self._observers = (*self._observers, (registration_id, observer))
        return ObserverHandle(self, registration_id)

    def publish(self, event: RuntimeEvent) -> None:
        """分发只读生命周期事件，记录观察者错误但不改变业务结果。

        Args:
            event: 需要发布、观察或处理的事件。
        """

        with self._lock:
            observers = self._observers
        for _, observer in observers:
            try:
                observer(event)
            except Exception:
                _LOGGER.exception(
                    "runtime observer failed for %s",
                    event.kind.value,
                )

    def _detach(self, registration_id: int) -> None:
        """从当前注册集合中移除指定注册项。

        Args:
            registration_id: 需要移除的唯一观察注册编号。
        """

        with self._lock:
            self._observers = tuple(
                item for item in self._observers if item[0] != registration_id
            )
