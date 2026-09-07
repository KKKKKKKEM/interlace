"""连接不同 Graph 的领域事件。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from .core import require_non_empty_string
from .slots import Slot


@dataclass(frozen=True, slots=True)
class Event:
    """按类型路由并携带领域 payload 的不可变消息。

    Attributes:
        type: 领域事件的路由类型。
        payload: 领域事件携带的业务数据。
    """

    type: str
    payload: Any = None

    def __post_init__(self) -> None:
        """校验事件类型是非空字符串。"""

        require_non_empty_string(self.type, "event type")


Emit = Callable[[Event], None]


def snapshot_options(options: Mapping[str, Any] | None) -> dict[str, Any]:
    """复制执行配置，避免调用方在提交后修改配置。

    Args:
        options: 使用领域命名空间键的配置映射，None 表示空配置。

    Returns:
        与输入独立的深复制字典。

    Raises:
        TypeError: 配置不是映射，或配置值无法深复制。
        ValueError: 配置键不是非空字符串。
    """

    if options is None:
        return {}
    if not isinstance(options, Mapping):
        raise TypeError("options must be a mapping or None")
    for key in options:
        require_non_empty_string(key, "option key")
    return deepcopy(dict(options))


class Context:
    """向当前 Node 暴露事件、Slot 和协作式执行检查点。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _emit: 事件发布回调。
        _slot: 当前逻辑执行链的本地执行资源。
        _checkpoint: 当前执行的协作式控制检查函数。
        _is_cancelled: 查询执行取消状态的函数。
        _local: 按作用域和命名空间隔离的执行局部存储。
        _finalizers: 当前执行静止阶段的回调集合。
        _scope: 当前节点或插件的状态隔离作用域。
        _options: 当前节点调用独立持有的执行配置，顶层只读。
    """

    __slots__ = (
        "_checkpoint",
        "_emit",
        "_finalizers",
        "_is_cancelled",
        "_local",
        "_scope",
        "_slot",
        "_options",
    )

    def __init__(
        self,
        emit: Emit,
        slot: Slot | None = None,
        *,
        checkpoint: Callable[[], None] | None = None,
        is_cancelled: Callable[[], bool] | None = None,
        local: MutableMapping[tuple[str, str], MutableMapping[str, Any]] | None = None,
        finalizers: list[Callable[[], None]] | None = None,
        scope: str = "",
        options: Mapping[str, Any] | None = None,
    ) -> None:
        """绑定 Runtime 的内部事件接收函数。

        Args:
            emit: 发布跨图事件的回调。
            slot: 当前逻辑执行链使用的本地执行槽。
            checkpoint: 协作式取消和超时检查函数。
            is_cancelled: 查询当前执行是否已经请求取消的回调。
            local: 按节点和插件命名空间隔离的执行局部存储。
            finalizers: 静止阶段按注册顺序调用的回调集合。
            scope: 当前回调或状态所属的隔离作用域。
            options: 本次执行配置；深复制后顶层只读，不随 emit 继承。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not callable(emit):
            raise TypeError("context emitter must be callable")
        if slot is not None and not isinstance(slot, Slot):
            raise TypeError("context slot must be a Slot or None")
        self._emit = emit
        self._slot = slot
        self._checkpoint = _noop if checkpoint is None else checkpoint
        self._is_cancelled = _false if is_cancelled is None else is_cancelled
        self._local = {} if local is None else local
        self._finalizers = [] if finalizers is None else finalizers
        self._scope = scope
        self._options = MappingProxyType(snapshot_options(options))
        if not callable(self._checkpoint) or not callable(self._is_cancelled):
            raise TypeError("context control callbacks must be callable")

    @property
    def options(self) -> Mapping[str, Any]:
        """读取本次执行配置；嵌套值的修改仅影响当前节点调用。

        Returns:
            顶层只读的领域配置映射，不包含运行时资源。
        """

        return self._options

    @property
    def slot(self) -> Slot | None:
        """返回随当前逻辑执行链传递的状态槽。

        Returns:
            当前逻辑链使用的本地 Slot。
        """

        return self._slot

    def emit(self, event_type: str, payload: Any = None) -> Event:
        """向 Runtime 提交一项跨 Graph 领域事件。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            payload: 事件携带的领域数据。

        Returns:
            事件传输已经接受的 Event 实例。
        """

        event = Event(event_type, payload)
        self._emit(event)
        return event

    @property
    def cancelled(self) -> bool:
        """返回当前 execution 是否已经收到取消请求。

        Returns:
            满足当前操作的判断条件时返回 True，否则返回 False。
        """

        return self._is_cancelled()

    def checkpoint(self) -> None:
        """让同步 Node 协作式响应取消、总超时和单 Node 超时。"""

        self._checkpoint()

    def state(self, namespace: str) -> MutableMapping[str, Any]:
        """返回当前 execution 内、按插件命名空间隔离的临时状态。

        Args:
            namespace: 当前领域组件的状态命名空间。

        Returns:
            当前作用域与命名空间对应的可变执行局部映射。
        """

        namespace = require_non_empty_string(namespace, "context state namespace")
        scoped = (self._scope, namespace)
        return self._local.setdefault(scoped, {})

    def on_quiescence(self, callback: Callable[[], None]) -> None:
        """注册一次 execution 静止后的校验或清理回调。

        Args:
            callback: 在对应生命周期边界调用的函数。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not callable(callback):
            raise TypeError("quiescence callback must be callable")
        self._finalizers.append(callback)


def _noop() -> None:
    """提供不执行额外控制检查的默认回调。"""

    return None


def _false() -> bool:
    """提供默认的未取消状态。

    Returns:
        固定值 False。
    """

    return False
