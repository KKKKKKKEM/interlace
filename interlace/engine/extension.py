"""统一扩展机制：两种钩子点、具名值与可注销的注册句柄。

核心与插件以同一方式定义钩子点：NoticePoint 只读通知且失败被隔离，WrapPoint 以中间件包裹一次操作，
可改写参数与结果、短路、重试或拒绝。具名值按命名空间名称保存可替换的实现，例如输入 selector。
HookBus 按 Runtime 隔离保存登记，每次调用读取登记快照，运行期可随时登记与注销。
"""

from __future__ import annotations

import inspect
import itertools
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from threading import RLock
from types import MappingProxyType
from typing import Any, Generic, TypeVar, cast

from .._validation import require_namespaced
from .errors import ExecutionControlError

_LOGGER = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")

_SCOPE_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class _Point:
    """钩子点与注册表种类的公共身份：命名空间 ID 与可过滤的作用域键。

    按对象身份比较与哈希；同名的两个对象是两个不同的点。

    Attributes:
        id: 带命名空间的唯一标识，用于诊断与错误信息。
        scope: 注册时可用于过滤、调用时必须全部提供的作用域键名，按声明顺序排列。
    """

    __slots__ = ("id", "scope")

    def __init__(self, id: str, *, scope: tuple[str, ...] = ()) -> None:
        """定义点的身份与作用域键。

        Args:
            id: 带命名空间的标识，例如 ``"interlace.work/deliver"``。
            scope: 作用域键名元组，每项须为合法标识符且不重复。

        Raises:
            TypeError: id 不是字符串或 scope 不是字符串元组。
            ValueError: id 未带命名空间，或 scope 含非法、重复键名。
        """

        self.id = require_namespaced(id, "point id")
        if not isinstance(scope, tuple) or not all(
            isinstance(key, str) for key in scope
        ):
            raise TypeError("scope must be a tuple of strings")
        if len(set(scope)) != len(scope):
            raise ValueError("scope keys must be unique")
        for key in scope:
            if not _SCOPE_KEY.fullmatch(key):
                raise ValueError(f"invalid scope key {key!r}")
        self.scope = scope

    def __repr__(self) -> str:
        """返回包含类型与 ID 的表示。"""

        return f"{type(self).__name__}({self.id!r})"


class NoticePoint(_Point, Generic[T]):
    """只读通知点：处理函数签名为 ``fn(value) -> None``，失败被记录并隔离。

    处理函数在触发方的调用路径上依次同步执行；慢速观察者可经 `interlace.plugins.Background`
    移出调用路径。
    """

    __slots__ = ()


class WrapPoint(_Point, Generic[T, R]):
    """中间件点：处理函数签名为 ``fn(value, call_next) -> result``，先注册者位于外层。

    处理函数可以修改传给 call_next 的值、改写结果、不调用 call_next 直接返回（短路）、多次调用（重试）
    或抛出异常拒绝本次操作。call_next 抛出的执行控制异常不能被吞掉：处理函数正常返回或改抛其他异常时，
    宿主重新抛出原控制异常。

    Attributes:
        value_type: 到达最内层操作的值必须满足的 isinstance 类型；None 表示不检查。
        required: 是否必须有处理函数；为 True 时调用作用域内没有生效的处理函数则拒绝本次操作，
            而不是直接执行最内层操作。
    """

    __slots__ = ("required", "value_type")

    def __init__(
        self,
        id: str,
        *,
        scope: tuple[str, ...] = (),
        value_type: type | tuple[type, ...] | None = None,
        required: bool = False,
    ) -> None:
        """定义中间件点。

        Args:
            id: 带命名空间的标识。
            scope: 作用域键名元组。
            value_type: 中间件改写后交给最内层操作的值须满足的类型；None 表示不检查。
            required: 没有生效的处理函数时是否拒绝调用，默认放行。

        Raises:
            TypeError: required 不是 bool。
        """

        super().__init__(id, scope=scope)
        if not isinstance(required, bool):
            raise TypeError("required must be a bool")
        self.value_type = value_type
        self.required = required


AnyPoint = NoticePoint[Any] | WrapPoint[Any, Any]


class Registration:
    """一项注册的统一注销句柄；可调用、可作上下文管理器，注销是幂等的。

    Attributes:
        owner: 登记该注册的插件 ID；直接通过 HookBus 注册时为 None。
        _detach: 首次注销时执行的回调，注销后置为 None。
        _lock: 保证注销回调只执行一次。
    """

    __slots__ = ("_detach", "_lock", "owner")

    def __init__(self, detach: Callable[[], None], *, owner: str | None = None) -> None:
        """包装注销回调。

        Args:
            detach: 撤销注册的回调，只会被调用一次。
            owner: 登记方插件 ID；None 表示直接注册。
        """

        if not callable(detach):
            raise TypeError("detach must be callable")
        self._detach: Callable[[], None] | None = detach
        self._lock = RLock()
        self.owner = owner

    @property
    def active(self) -> bool:
        """注册是否尚未注销。"""

        with self._lock:
            return self._detach is not None

    def detach(self) -> None:
        """撤销注册；重复调用无作用。"""

        with self._lock:
            detach, self._detach = self._detach, None
        if detach is not None:
            detach()

    def __call__(self) -> None:
        """等同于 detach，使句柄可直接用作注销函数。"""

        self.detach()

    def __enter__(self) -> Registration:
        """进入作用域，返回自身。"""

        return self

    def __exit__(self, *exc_info: object) -> None:
        """离开作用域时注销。"""

        self.detach()


@dataclass(frozen=True, slots=True, eq=False)
class _Entry:
    """一项钩子或注册表登记。

    Attributes:
        order: 全局单调递增的登记序号，决定同一点内的调用顺序。
        value: 处理函数或注册表值。
        scope: 注册时声明的作用域过滤条件，只读映射。
        owner: 登记方插件 ID；None 表示直接注册。
    """

    order: int
    value: Any
    scope: Mapping[str, Any] = field(default_factory=dict)
    owner: str | None = None

    def matches(self, scope: Mapping[str, Any]) -> bool:
        """判断调用作用域是否满足本登记的全部过滤条件。"""

        return all(scope.get(key) == expected for key, expected in self.scope.items())


def _check_scope(point: _Point, scope: Mapping[str, Any], *, full: bool) -> None:
    """校验作用域键；full=True 时要求提供点声明的全部键。

    Raises:
        ValueError: 出现未声明的键，或 full=True 时缺少已声明的键。
    """

    unknown = set(scope) - set(point.scope)
    if unknown:
        raise ValueError(f"{point.id} has no scope keys {sorted(unknown)!r}")
    if full:
        missing = [key for key in point.scope if key not in scope]
        if missing:
            raise ValueError(f"{point.id} call requires scope keys {missing!r}")


def _is_control(error: BaseException) -> bool:
    """判断异常是否属于处理函数不得吞掉的控制异常。"""

    return isinstance(error, ExecutionControlError) or not isinstance(error, Exception)


class HookCaller:
    """只允许触发钩子点的视图，供 Context 等领域代码使用，不能注册或注销。

    Attributes:
        _bus: 被委托的 HookBus。
    """

    __slots__ = ("_bus",)

    def __init__(self, bus: HookBus) -> None:
        """绑定被委托的 HookBus。"""

        self._bus = bus

    def notify(self, point: NoticePoint[T], value: T, /, **scope: Any) -> None:
        """见 `HookBus.notify`。"""

        self._bus.notify(point, value, **scope)

    def wrap(
        self,
        point: WrapPoint[T, R],
        value: T,
        call: Callable[[T], R],
        /,
        **scope: Any,
    ) -> R:
        """见 `HookBus.wrap`。"""

        return self._bus.wrap(point, value, call, **scope)

    async def anotify(self, point: NoticePoint[T], value: T, /, **scope: Any) -> None:
        """见 `HookBus.anotify`。"""

        await self._bus.anotify(point, value, **scope)

    async def awrap(
        self,
        point: WrapPoint[T, R],
        value: T,
        call: Callable[[T], Any],
        /,
        **scope: Any,
    ) -> R:
        """见 `HookBus.awrap`。"""

        return await self._bus.awrap(point, value, call, **scope)


class HookBus:
    """一个 Runtime 内全部钩子与具名值的线程安全存储与分发器。

    每个点的登记以写时复制元组保存；调用读取当时的快照，进行中的调用不受并发注册或注销影响。
    同一点内按登记顺序调用：Wrap 先登记者位于外层，Notice 依次通知。

    Attributes:
        _lock: 保护登记表、具名值与序号。
        _entries: 点到登记元组的映射，元组按登记顺序排列。
        _values: 具名值名称到 (值, 登记方插件 ID) 的映射，按登记顺序排列。
        _order: 登记序号生成器。
    """

    def __init__(self) -> None:
        """创建空的钩子总线。"""

        self._lock = RLock()
        self._entries: dict[_Point, tuple[_Entry, ...]] = {}
        self._values: dict[str, tuple[Any, str | None]] = {}
        self._order = itertools.count()

    def caller(self) -> HookCaller:
        """返回只能触发钩子点的视图。"""

        return HookCaller(self)

    def on(
        self,
        point: AnyPoint,
        handler: Callable[..., Any],
        /,
        *,
        owner: str | None = None,
        **scope: Any,
    ) -> Registration:
        """在钩子点登记处理函数，从下一次调用开始生效。

        Args:
            point: NoticePoint 或 WrapPoint。
            handler: 符合点签名的同步可调用对象；异步触发入口也接受返回 awaitable 的函数。
            owner: 登记方插件 ID，仅用于诊断与批量注销。
            **scope: 作用域过滤条件；只在调用作用域对应值相等时生效。

        Returns:
            撤销本次登记的句柄。

        Raises:
            TypeError: point 不是钩子点或 handler 不可调用。
            ValueError: scope 含未声明的键。
        """

        if not isinstance(point, NoticePoint | WrapPoint):
            raise TypeError("point must be a NoticePoint or WrapPoint")
        if not callable(handler):
            raise TypeError("hook handler must be callable")
        return self._add(point, handler, scope, owner)

    def provide(
        self, name: str, value: Any, /, *, owner: str | None = None
    ) -> Registration:
        """以命名空间名称提供一项具名值，例如输入 selector；读取方在使用时查找。

        Args:
            name: 带命名空间的名称，同一总线内唯一。
            value: 提供的值，由读取方按名称约定解释。
            owner: 登记方插件 ID。

        Returns:
            撤销本次提供的句柄。

        Raises:
            ValueError: name 未带命名空间或已被提供。
        """

        name = require_namespaced(name, "provided name")
        if owner is not None:
            require_namespaced(owner, "owner")
        with self._lock:
            if name in self._values:
                raise ValueError(f"{name!r} is already provided")
            entry = (value, owner)
            self._values[name] = entry

        def withdraw() -> None:
            """撤销本次提供；名称已被重新提供时不影响新值。"""

            with self._lock:
                if self._values.get(name) is entry:
                    del self._values[name]

        return Registration(withdraw, owner=owner)

    def lookup(self, name: str, /) -> Any:
        """取得当前提供的具名值。

        Raises:
            LookupError: name 尚未被提供。
        """

        with self._lock:
            try:
                return self._values[name][0]
            except KeyError:
                raise LookupError(f"{name!r} is not provided") from None

    def provided(self) -> Mapping[str, Any]:
        """返回当前全部具名值的只读快照，按提供顺序排列。"""

        with self._lock:
            return MappingProxyType(
                {name: value for name, (value, _) in self._values.items()}
            )

    def handlers(
        self, point: AnyPoint, /, **scope: Any
    ) -> tuple[Callable[..., Any], ...]:
        """返回在给定调用作用域下生效的处理函数快照，按登记顺序排列。

        供需要自行驱动调用链的宿主使用，例如流式的节点中间件。

        Raises:
            ValueError: scope 含未声明的键或缺少已声明的键。
            LookupError: required 的 WrapPoint 在该作用域下没有生效的处理函数。
        """

        _check_scope(point, scope, full=True)
        with self._lock:
            entries = self._entries.get(point, ())
        handlers = tuple(entry.value for entry in entries if entry.matches(scope))
        if not handlers and isinstance(point, WrapPoint) and point.required:
            raise LookupError(f"{point.id} requires a registered handler")
        return handlers

    def detach_owner(self, owner: str) -> None:
        """注销某个插件登记的全部钩子与具名值。"""

        with self._lock:
            for point, entries in tuple(self._entries.items()):
                kept = tuple(entry for entry in entries if entry.owner != owner)
                self._entries[point] = kept
            for name, (_, value_owner) in tuple(self._values.items()):
                if value_owner == owner:
                    del self._values[name]

    def notify(self, point: NoticePoint[T], value: T, /, **scope: Any) -> None:
        """依次通知处理函数；普通异常记录后隔离，控制异常继续传播。

        Raises:
            TypeError: 处理函数返回了 awaitable。
        """

        for handler in self.handlers(point, **scope):
            try:
                result = handler(value)
                _reject_awaitable(result, point)
            except Exception:
                _LOGGER.exception("notice handler of %s failed", point.id)

    def wrap(
        self,
        point: WrapPoint[T, R],
        value: T,
        call: Callable[[T], R],
        /,
        **scope: Any,
    ) -> R:
        """以登记的中间件包裹 call 并执行；没有处理函数时直接调用 call(value)。

        Raises:
            TypeError: 处理函数返回 awaitable，或到达 call 的值不满足 point.value_type。
            LookupError: point 是 required 且没有生效的处理函数；此时不调用 call。
        """

        handlers = self.handlers(point, **scope)

        def invoke(index: int, current: T) -> R:
            """执行第 index 层中间件；越过最后一层时调用 call。"""

            if index == len(handlers):
                _check_value(point, current)
                return call(current)
            guard = _ControlGuard(lambda item: invoke(index + 1, item))
            try:
                result = handlers[index](current, guard)
            except BaseException as error:
                guard.reraise(error)
                raise
            guard.reraise(None)
            _reject_awaitable(result, point)
            return cast(R, result)

        return invoke(0, value)

    async def anotify(self, point: NoticePoint[T], value: T, /, **scope: Any) -> None:
        """异步版 notify：处理函数可以是同步函数或协程函数。"""

        for handler in self.handlers(point, **scope):
            try:
                result = handler(value)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                _LOGGER.exception("notice handler of %s failed", point.id)

    async def awrap(
        self,
        point: WrapPoint[T, R],
        value: T,
        call: Callable[[T], Any],
        /,
        **scope: Any,
    ) -> R:
        """异步版 wrap：call 与中间件可以是同步函数或协程函数，call_next 总是返回 awaitable。

        Raises:
            LookupError: point 是 required 且没有生效的处理函数；此时不调用 call。
        """

        handlers = self.handlers(point, **scope)

        async def invoke(index: int, current: T) -> R:
            """执行第 index 层中间件；越过最后一层时调用并等待 call。"""

            if index == len(handlers):
                _check_value(point, current)
                result = call(current)
                return cast(R, await result if inspect.isawaitable(result) else result)
            guard = _ControlGuard(lambda item: invoke(index + 1, item))
            try:
                result = handlers[index](current, guard)
                if inspect.isawaitable(result):
                    result = await result
            except BaseException as error:
                guard.reraise(error)
                raise
            guard.reraise(None)
            return cast(R, result)

        return await invoke(0, value)

    def _add(
        self,
        point: _Point,
        value: Any,
        scope: Mapping[str, Any],
        owner: str | None,
    ) -> Registration:
        """登记一项并返回注销句柄。"""

        _check_scope(point, scope, full=False)
        if owner is not None:
            require_namespaced(owner, "owner")
        with self._lock:
            entry = _Entry(
                next(self._order), value, MappingProxyType(dict(scope)), owner
            )
            self._entries[point] = (*self._entries.get(point, ()), entry)

        def detach() -> None:
            """从登记表移除本项。"""

            with self._lock:
                entries = self._entries.get(point, ())
                self._entries[point] = tuple(
                    item for item in entries if item is not entry
                )

        return Registration(detach, owner=owner)


class _ControlGuard:
    """传给中间件的 call_next：记录内层抛出的控制异常，防止中间件吞掉后继续。

    Attributes:
        _call: 调用内层的函数。
        _control: 内层最近一次抛出的控制异常；未发生时为 None。
    """

    __slots__ = ("_call", "_control")

    def __init__(self, call: Callable[[Any], Any]) -> None:
        """绑定内层调用。"""

        self._call = call
        self._control: BaseException | None = None

    def __call__(self, value: Any) -> Any:
        """调用内层，并记录其抛出的控制异常后原样传播。"""

        try:
            result = self._call(value)
        except BaseException as error:
            if _is_control(error):
                self._control = error
            raise
        if inspect.isawaitable(result):
            return self._await(result)
        return result

    async def _await(self, awaitable: Any) -> Any:
        """等待异步内层结果，同样记录控制异常。"""

        try:
            return await awaitable
        except BaseException as error:
            if _is_control(error):
                self._control = error
            raise

    def reraise(self, raised: BaseException | None) -> None:
        """中间件结束时，若内层控制异常被吞掉或替换，重新抛出该控制异常。"""

        control = self._control
        if control is not None and raised is not control:
            raise control


def _reject_awaitable(value: object, point: _Point) -> None:
    """拒绝同步入口中处理函数返回的 awaitable，避免协程被静默丢弃。"""

    if inspect.isawaitable(value):
        close = getattr(value, "close", None)
        if callable(close):
            close()
        raise TypeError(f"{point.id} handlers must be synchronous here")


def _check_value(point: WrapPoint[Any, Any], value: object) -> None:
    """按 WrapPoint.value_type 检查中间件交给最内层操作的值。"""

    if point.value_type is not None and not isinstance(value, point.value_type):
        raise TypeError(
            f"{point.id} middleware passed {type(value).__name__}, "
            f"expected {point.value_type!r}"
        )
