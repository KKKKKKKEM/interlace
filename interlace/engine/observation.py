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
        EXECUTION_STARTED: 执行宿主开始一次 execution。
        EXECUTION_FINISHED: 执行器结束后发布，status 为即将写入的终态；观察者收到时
            `Execution.status` 可能仍为 RUNNING。
        NODE_STARTED: 一次 Node firing 计步后、调用 Context 工厂前发布。
        NODE_FINISHED: 一次 Node firing 结束后发布，status 为 succeeded 或 failed。
        OUTPUT_ROUTED: 输出通过校验后的端口传播记录，不携带业务值。
        EVENT_PUBLISHED: Router 开始向 EventBus 分发领域事件，先于该事件引起的
            WORK_SUBMITTED；分发失败由 emit 抛出 EventDispatchError，不撤回本记录。
        WORK_SUBMITTED: route 已把事件转换的 Work 交给任务发布端。
        WORK_FINISHED: 一项 Work 交付结束。
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


class RuntimeObserver(Protocol):
    """接收只读生命周期事件的回调协议。

    ObservationHub 在发布事件的线程上同步、按注册顺序调用观察者；Node 与输出事件
    位于执行该 Graph 的线程。观察者阻塞会直接延迟业务执行，耗时处理应自行转交队列；
    抛出的 Exception 被记录并忽略。
    """

    def __call__(self, event: RuntimeEvent) -> None:
        """处理一项生命周期事件；返回值被忽略。"""


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
        __slots__: 固定实例字段。
        _hub: 注册所属的分发中心。
        _registration_id: 本句柄独立拥有的注册编号。
    """

    __slots__ = ("_hub", "_registration_id")

    def __init__(self, hub: ObservationHub, registration_id: int) -> None:
        """绑定注册编号及其分发中心。"""

        self._hub = hub
        self._registration_id = registration_id

    def detach(self) -> None:
        """幂等解除当前注册，不影响相同回调的其他注册。"""

        self._hub._detach(self._registration_id)


class CompositeObserverHandle:
    """统一卸载多个观察者注册的组合句柄。

    Attributes:
        __slots__: 固定实例字段。
        _handles: 需要统一卸载的观察句柄。
    """

    __slots__ = ("_handles",)

    def __init__(self, handles: tuple[ObserverHandle, ...]) -> None:
        """保存需要统一卸载的观察句柄。"""

        self._handles = handles

    def detach(self) -> None:
        """依次卸载全部注册；每个句柄自身幂等。"""

        for handle in self._handles:
            handle.detach()


class ObservationHub:
    """线程安全地分发只读事件；观察者失败不会影响业务执行。

    Attributes:
        _observers: 注册编号与观察者的不可变快照，按注册顺序排列。
        _counter: 分配注册编号的单调计数器。
        _lock: 保护快照替换的锁；分发时不持有。
    """

    def __init__(self) -> None:
        """创建空的观察者快照。"""

        self._observers: tuple[tuple[int, RuntimeObserver], ...] = ()
        self._counter = itertools.count()
        self._lock = RLock()

    def attach(self, observer: RuntimeObserver) -> ObserverHandle:
        """注册只读观察者并返回独立卸载句柄；同一回调可注册多次。

        Args:
            observer: 接收 RuntimeEvent 的可调用对象。

        Returns:
            只卸载本次注册的句柄。

        Raises:
            TypeError: observer 不可调用。
        """

        if not callable(observer):
            raise TypeError("runtime observer must be callable")
        with self._lock:
            registration_id = next(self._counter)
            self._observers = (*self._observers, (registration_id, observer))
        return ObserverHandle(self, registration_id)

    def publish(self, event: RuntimeEvent) -> None:
        """在当前线程按注册顺序同步调用全部观察者。

        使用调用开始时的注册快照；观察者抛出的 Exception 被记录后继续调用其余观察者。

        Args:
            event: 需要分发的生命周期事件。
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
        """移除指定注册；编号不存在时无作用。"""

        with self._lock:
            self._observers = tuple(
                item for item in self._observers if item[0] != registration_id
            )
