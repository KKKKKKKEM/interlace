"""连接不同 Graph 的领域事件与节点调用上下文。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, TypeVar

from .._validation import require_non_empty_string
from .extension import HookBus, HookCaller
from .options import snapshot_options
from .slots import Slot


@dataclass(frozen=True, slots=True)
class Event:
    """按类型路由并携带领域 payload 的不可变消息。

    Event 按值比较：type 与 payload 都相等时两个 Event 相等。哈希同样基于这两个字段，
    因此只有 payload 可哈希时 Event 才可哈希；payload 为 dict 等可变容器时 ``hash()``
    抛出 TypeError。Runtime 不比较或去重 Event，不依赖其可哈希性。

    Attributes:
        type: 领域事件的路由类型，非空字符串。
        payload: 领域事件携带的业务数据，默认 None。
    """

    type: str
    payload: Any = None

    def __post_init__(self) -> None:
        """校验事件类型是非空字符串。"""

        require_non_empty_string(self.type, "event type")


Emit = Callable[[Event], object]  # 发布跨图事件的回调；返回值被忽略。
_C = TypeVar("_C", bound="Context")  # 保留领域工厂返回的具体 Context 子类类型。


class Context:
    """向当前 Node 暴露事件、Slot 和协作式执行检查点。

    公开构造只接受 parent：领域类型通过 ``Context(parent=base)`` 或
    ``super().__init__(parent=base)`` 组合引擎绑定的基础上下文，全部执行能力逐级委托给
    parent，不复制其状态。基础上下文由执行器通过内部的 ``bind_context`` 创建。
    每次节点调用创建独立实例；附加字段由工厂或领域节点绑定，不自动跨节点或 execution 传播。

    Attributes:
        __slots__: 限制本类只保存 parent；未声明 ``__slots__`` 的领域子类仍可附加字段。
        _parent: 被组合的基础上下文，执行能力完全委托给它。
    """

    __slots__ = ("_parent",)

    def __init__(self, *, parent: Context) -> None:
        """组合基础上下文的执行能力。

        Args:
            parent: 被组合的上下文，通常是工厂收到的基础 Context。

        Raises:
            TypeError: parent 不是 Context。
        """

        if not isinstance(parent, Context):
            raise TypeError("parent must be a Context")
        self._parent = parent

    @classmethod
    def make(cls: type[_C], base: Context, inputs: Mapping[str, Any]) -> _C:
        """默认 Context 工厂：返回委托给 base 的新实例。

        领域工厂可以独立实现本方法并调用自己的构造器；构造器通过
        ``super().__init__(parent=base)`` 组合执行能力。

        Args:
            base: 引擎为本次 firing 绑定的基础上下文。
            inputs: 本次 firing 选中的只读端口映射，位于 Hook enter 转换之前；默认实现不读取。

        Returns:
            当前类的新实例，执行能力委托给 base。

        Raises:
            TypeError: base 不是 Context、inputs 不是映射，或当前类的构造器不接受 parent。
        """

        if not isinstance(base, Context):
            raise TypeError("base must be a Context")
        if not isinstance(inputs, Mapping):
            raise TypeError("inputs must be a mapping")
        return cls(parent=base)

    @property
    def options(self) -> Mapping[str, Any]:
        """返回本次节点调用的执行配置快照。

        Returns:
            顶层只读的配置映射；嵌套值是本次 firing 独立的深复制，修改仅影响当前调用。
        """

        return self._parent.options

    @property
    def slot(self) -> Slot | None:
        """返回随当前逻辑执行链传递的 Slot。

        Returns:
            当前逻辑链使用的 Slot；直接执行且未提供 Slot 时为 None。
        """

        return self._parent.slot

    @property
    def hooks(self) -> HookCaller:
        """返回当前 Runtime 钩子总线的只触发视图，供领域代码调用插件定义的钩子点。

        Returns:
            只能 notify/filter/wrap 及其异步版本的视图，不能注册或注销。
        """

        return self._parent.hooks

    def emit(self, event_type: str, payload: Any = None) -> Event:
        """向 Runtime 提交一项跨 Graph 领域事件。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            payload: 事件携带的领域数据。

        Returns:
            事件传输已经接受的 Event 实例。

        Raises:
            TypeError: event_type 不是字符串。
            ValueError: event_type 为空。
        """

        return self._parent.emit(event_type, payload)

    @property
    def cancelled(self) -> bool:
        """返回当前 execution 是否已经收到取消请求。

        Returns:
            已请求取消时返回 True。
        """

        return self._parent.cancelled

    def checkpoint(self) -> None:
        """让同步 Node 协作式响应取消、总超时和单 Node 超时。

        Raises:
            ExecutionControlError: 当前 execution 已取消、超时或超过 Node 时限。
        """

        self._parent.checkpoint()

    def state(self, namespace: str) -> MutableMapping[str, Any]:
        """返回当前 execution 内、按 Node 与命名空间隔离的临时状态。

        Args:
            namespace: 当前领域组件的状态命名空间。

        Returns:
            当前 Node 作用域与命名空间对应的可变映射，同一 execution 内重复调用返回同一对象。

        Raises:
            TypeError: namespace 不是字符串。
            ValueError: namespace 为空。
        """

        return self._parent.state(namespace)

    def on_quiescence(self, callback: Callable[[], None]) -> None:
        """注册本次 execution 数据流自然静止时运行的校验或清理回调。

        仅当 Graph 不再有可触发的 Node 且没有残留输入时，执行器在 execution 成功结束前按注册顺序
        同步调用全部回调，包括回调运行期间新注册的回调；每个回调前检查取消与超时。Node 失败、取消、
        超时、步数限制、残留输入或 Hook 以 StopGraph 停止时不调用。回调抛出的执行控制异常原样传播；
        其他 ExecutionError 补齐 graph 后传播；普通异常包装为带 graph 的 ExecutionError。首个失败
        终止后续回调并使 execution 失败。

        Args:
            callback: 无参回调，返回值被忽略。

        Raises:
            TypeError: callback 不可调用。
        """

        self._parent.on_quiescence(callback)


class _BoundContext(Context):
    """执行器为一次 Node firing 绑定的根上下文，直接持有执行能力。

    覆盖 Context 的全部能力接口，不读取继承的 ``_parent`` 槽位。

    Attributes:
        _emit: 接收已构造 Event 的发布回调。
        _slot: 当前逻辑执行链的 Slot；没有时为 None。
        _checkpoint: 协作式取消与超时检查函数。
        _is_cancelled: 查询 execution 取消状态的函数。
        _local: execution 共享、按 (作用域, 命名空间) 索引的状态存储；由执行器拥有。
        _finalizers: execution 共享的静止回调列表；由执行器拥有并调用。
        _scope: 当前 Node 的状态隔离作用域。
        _options: 本次 firing 独立深复制的执行配置，顶层只读。
        _hooks: 当前 Runtime 钩子总线的只触发视图。
    """

    __slots__ = (
        "_checkpoint",
        "_emit",
        "_finalizers",
        "_hooks",
        "_is_cancelled",
        "_local",
        "_options",
        "_scope",
        "_slot",
    )

    def __init__(
        self,
        emit: Emit,
        slot: Slot | None,
        checkpoint: Callable[[], None],
        is_cancelled: Callable[[], bool],
        local: MutableMapping[tuple[str, str], MutableMapping[str, Any]],
        finalizers: list[Callable[[], None]],
        scope: str,
        options: Mapping[str, Any] | None,
        hooks: HookCaller,
    ) -> None:
        """保存已由 bind_context 校验的执行绑定。"""

        self._emit = emit
        self._slot = slot
        self._checkpoint = checkpoint
        self._is_cancelled = is_cancelled
        self._local = local
        self._finalizers = finalizers
        self._scope = scope
        self._options = MappingProxyType(snapshot_options(options))
        self._hooks = hooks

    @property
    def options(self) -> Mapping[str, Any]:
        """返回本次 firing 的只读配置快照。"""

        return self._options

    @property
    def slot(self) -> Slot | None:
        """返回绑定的 Slot。"""

        return self._slot

    @property
    def hooks(self) -> HookCaller:
        """返回绑定的钩子触发视图。"""

        return self._hooks

    def emit(self, event_type: str, payload: Any = None) -> Event:
        """构造 Event 并交给绑定的发布回调。"""

        event = Event(event_type, payload)
        self._emit(event)
        return event

    @property
    def cancelled(self) -> bool:
        """查询绑定的取消状态。"""

        return self._is_cancelled()

    def checkpoint(self) -> None:
        """调用绑定的协作式检查函数。"""

        self._checkpoint()

    def state(self, namespace: str) -> MutableMapping[str, Any]:
        """返回 (作用域, namespace) 对应的状态映射，不存在时创建。"""

        namespace = require_non_empty_string(namespace, "context state namespace")
        return self._local.setdefault((self._scope, namespace), {})

    def on_quiescence(self, callback: Callable[[], None]) -> None:
        """把回调追加到 execution 共享的静止回调列表。"""

        if not callable(callback):
            raise TypeError("quiescence callback must be callable")
        self._finalizers.append(callback)


def bind_context(
    emit: Emit,
    slot: Slot | None,
    *,
    checkpoint: Callable[[], None] | None = None,
    is_cancelled: Callable[[], bool] | None = None,
    local: MutableMapping[tuple[str, str], MutableMapping[str, Any]] | None = None,
    finalizers: list[Callable[[], None]] | None = None,
    scope: str = "",
    options: Mapping[str, Any] | None = None,
    hooks: HookCaller | None = None,
) -> Context:
    """为一次 Node firing 创建直接持有执行能力的基础 Context。

    仅供执行器与替代 GraphExecutor 使用，不属于顶层 API。

    Args:
        emit: 接收已构造 Event 的发布回调。
        slot: 当前逻辑执行链的 Slot；没有时为 None。
        checkpoint: 协作式取消与超时检查函数；None 表示不检查。
        is_cancelled: 查询取消状态的函数；None 表示始终未取消。
        local: execution 共享的状态存储，按 (作用域, 命名空间) 索引；None 时新建独立存储。
        finalizers: execution 共享的静止回调列表，调用方负责执行；None 时新建独立列表。
        scope: 当前 Node 的状态隔离作用域，通常为 Node ID。
        options: 执行配置；深复制后顶层只读。
        hooks: 当前 Runtime 钩子总线的只触发视图；None 时使用空总线。

    Returns:
        绑定上述能力的基础 Context。

    Raises:
        TypeError: emit、checkpoint 或 is_cancelled 不可调用，slot 不是 Slot 或 None，
            hooks 不是 HookCaller，options 不是映射或其值无法深复制。
        ValueError: options 的键不是非空字符串。
    """

    if not callable(emit):
        raise TypeError("context emitter must be callable")
    if slot is not None and not isinstance(slot, Slot):
        raise TypeError("context slot must be a Slot or None")
    checkpoint = _noop if checkpoint is None else checkpoint
    is_cancelled = _false if is_cancelled is None else is_cancelled
    if not callable(checkpoint) or not callable(is_cancelled):
        raise TypeError("context control callbacks must be callable")
    if hooks is None:
        hooks = HookBus().caller()
    elif not isinstance(hooks, HookCaller):
        raise TypeError("context hooks must be a HookCaller")
    return _BoundContext(
        emit,
        slot,
        checkpoint,
        is_cancelled,
        {} if local is None else local,
        [] if finalizers is None else finalizers,
        scope,
        options,
        hooks,
    )


def _noop() -> None:
    """不执行任何检查的默认 checkpoint。"""


def _false() -> bool:
    """始终报告未取消的默认取消查询。"""

    return False


ContextFactory = Callable[[Context, Mapping[str, Any]], Context]
