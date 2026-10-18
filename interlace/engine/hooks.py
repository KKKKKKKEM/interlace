"""可热插拔的 Node 执行钩子和流程控制信号。"""

from __future__ import annotations

import enum
import itertools
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from threading import RLock
from types import MappingProxyType
from typing import Any

from .core import Node, Output
from .events import Context

Outputs = Output | Iterable[Output] | None


@dataclass(frozen=True, slots=True)
class NodeCall:
    """一次 Node 调用中允许 Hook 查看和修改的参数。

    Attributes:
        graph: Graph 的注册名称。
        node_id: Graph 内的节点 ID。
        node: 被调用的 Node 实例。
        inputs: 传给 Node 的只读端口输入映射。
        context: 本次 firing 的 Context，Node 与全部 Hook 共用同一实例。
    """

    graph: str
    node_id: str
    node: Node
    inputs: Mapping[str, Any]
    context: Context

    def with_inputs(self, inputs: Mapping[str, Any]) -> NodeCall:
        """保留调用身份，只替换传给 Node 的输入。

        Args:
            inputs: 新的端口输入；端口集合与值类型由 Engine 在调用前校验。

        Returns:
            身份不变、输入为只读副本的新 NodeCall。

        Raises:
            TypeError: inputs 不是 Mapping。
        """

        if not isinstance(inputs, Mapping):
            raise TypeError("hook inputs must be a mapping")
        return replace(self, inputs=MappingProxyType(dict(inputs)))


class NodeHook:
    """通过 enter、exit、error 和 inspect 介入一次 Node firing；默认实现不做改动。

    enter 按注册顺序执行，exit 与 error 按逆序执行；方法可返回 awaitable，由 Engine 等待。
    """

    def inspect(self, call: NodeCall, phase: HookPhase, value: Any) -> None:
        """同步采集实际 Node 调用边界；返回值不参与业务流程，失败被隔离。

        Args:
            call: 已经过全部 enter 转换的实际调用，只允许读取。
            phase: ENTER 表示实际调用前，EXIT 表示一项原始 Node 输出，ERROR 表示调用或迭代失败。
            value: ENTER 为 None，EXIT 为 Output，ERROR 为异常；不得修改原对象。
        """

        del call, phase, value

    def enter(self, call: NodeCall) -> NodeCall | Awaitable[NodeCall]:
        """在 Node 执行前转换调用参数，可抛出 ShortCircuit 或 StopGraph。

        Args:
            call: 外层 Hook 转换后的调用。

        Returns:
            保持身份不变的 NodeCall 或得到它的 awaitable；默认原样返回。
        """

        return call

    def exit(self, call: NodeCall, output: Output) -> Outputs | Awaitable[Outputs]:
        """转换一项 Node 或短路输出，可保留、丢弃或展开为多项。

        Args:
            call: Hook 当前处理的节点调用记录。
            output: Engine 已从源中读取的当前输出；Hook 不推进源迭代器。

        Returns:
            一项 Output、惰性 Iterable 或 None；None 丢弃当前项。
        """

        del call
        return output

    def error(self, call: NodeCall, error: Exception) -> Outputs | Awaitable[Outputs]:
        """处理内层 Node 或 Hook 的业务异常；默认重新抛出。

        执行控制异常与 Engine 契约错误（HookExecutionError、InvalidOutputError、
        PortValueTypeError、PolicySelectionError）不会交给本方法。

        Args:
            call: 本 Hook enter 返回的调用。
            error: 内层抛出的业务异常；已交付的输出前缀不会重放。

        Returns:
            恢复输出，可为 Output、Iterable[Output] 或 None；继续经过当前及外层出口转换。
        """

        del call
        raise error


class HookPhase(str, enum.Enum):
    """NodeHook 生命周期阶段，用于函数 Hook 与 inspect。

    Attributes:
        ENTER: Node 调用前。
        EXIT: 一项输出离开 Node 之后。
        ERROR: Node 调用或迭代失败之后。
    """

    ENTER = "enter"
    EXIT = "exit"
    ERROR = "error"


class HookSignal(BaseException):
    """由 Hook 抛出、Engine 捕获并解释的流程控制信号。

    继承 BaseException，避免被业务代码的 `except Exception` 吞掉。

    Attributes:
        outputs: 信号携带的 Output，按交付顺序排列。
    """

    def __init__(self, *outputs: Output) -> None:
        """保存信号携带的输出。

        Args:
            *outputs: 按交付顺序排列的 Output，由 Engine 校验。
        """

        super().__init__(*outputs)
        self.outputs = tuple(outputs)


class ShortCircuit(HookSignal):
    """跳过当前 Node，并把携带结果作为该 Node 的输出继续路由。"""


class StopGraph(HookSignal):
    """停止后续 Node，并把携带结果追加为 Graph 的终端输出。

    携带的 Output 按停止节点声明的输出端口与类型校验，但不沿 Edge 路由。
    """


@dataclass(frozen=True, slots=True)
class _Registration:
    """一个具有 Graph 和节点作用域的 Hook 注册记录。

    Attributes:
        id: 注册编号，HookRegistry 内唯一。
        hook: 注册的 NodeHook。
        graph: 生效的 Graph 注册名称；None 表示全部 Graph。
        node: 生效的节点 ID；None 表示 Graph 内全部节点。
    """

    id: int
    hook: NodeHook
    graph: str | None
    node: str | None


class HookHandle:
    """控制一项动态 Hook 注册，可用作上下文管理器。

    Attributes:
        __slots__: 固定实例字段。
        _registry: 注册所属的 HookRegistry。
        _registration_id: 本句柄负责卸载的注册编号。
    """

    __slots__ = ("_registration_id", "_registry")

    def __init__(self, registry: HookRegistry, registration_id: int) -> None:
        """绑定注册编号及其所属注册表。"""

        self._registry = registry
        self._registration_id = registration_id

    def detach(self) -> None:
        """幂等卸载；已经开始的 Graph execution 继续使用旧快照。"""

        self._registry._detach(self._registration_id)

    def __enter__(self) -> HookHandle:
        """返回自身，退出作用域时卸载。"""

        return self

    def __exit__(self, *args: object) -> None:
        """卸载本次注册。"""

        del args
        self.detach()


class _FunctionHook(NodeHook):
    """把单阶段函数适配为完整 NodeHook。

    Attributes:
        _function: 调用方提供的单阶段函数。
        _phase: 函数生效的阶段；其他阶段保持默认行为。
    """

    def __init__(self, function: Callable[..., object], phase: HookPhase) -> None:
        """绑定函数及其生效阶段。"""

        self._function = function
        self._phase = phase

    def enter(self, call: NodeCall) -> Any:
        """ENTER 阶段调用 `function(call)`，否则原样返回 call。"""

        if self._phase is HookPhase.ENTER:
            return self._function(call)
        return call

    def exit(self, call: NodeCall, output: Output) -> Any:
        """EXIT 阶段调用 `function(call, output)`，否则原样返回 output。"""

        if self._phase is HookPhase.EXIT:
            return self._function(call, output)
        return output

    def error(self, call: NodeCall, error: Exception) -> Any:
        """ERROR 阶段调用 `function(call, error)`，否则重新抛出 error。"""

        if self._phase is HookPhase.ERROR:
            return self._function(call, error)
        raise error


class HookRegistry:
    """线程安全地注册 Hook，并为 Graph execution 提供不可变快照。

    Attributes:
        _lock: 保护注册快照替换与关闭状态的锁。
        _counter: 分配注册编号的单调计数器。
        _registrations: 按注册顺序排列的不可变注册快照。
        _closed: 是否已关闭；关闭后拒绝新注册。
    """

    def __init__(self) -> None:
        """创建空注册表。"""

        self._lock = RLock()
        self._counter = itertools.count()
        self._registrations: tuple[_Registration, ...] = ()
        self._closed = False

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """按注册顺序挂载 NodeHook 或单阶段函数 Hook。

        Args:
            hook: NodeHook 实例，或只实现一个阶段的可调用对象。
            phase: 函数 Hook 的阶段，缺省为 ENTER；NodeHook 不接受此参数。
            graph: 只对该注册名称的 Graph 生效；None 表示全部 Graph。
            node: 只对该节点 ID 生效，必须同时提供 graph。

        Returns:
            卸载本次注册的句柄；已开始的 execution 继续使用旧快照。

        Raises:
            RuntimeError: 注册表已关闭。
            TypeError: hook 不可调用，或为 NodeHook 时提供了 phase。
            ValueError: 提供 node 但缺少 graph，或 phase 不是合法阶段。
        """

        if node is not None and graph is None:
            raise ValueError("node-scoped hook requires graph")
        if isinstance(hook, NodeHook):
            if phase is not None:
                raise TypeError("phase is only valid for a function hook")
            adapted = hook
        elif callable(hook):
            selected = HookPhase.ENTER if phase is None else HookPhase(phase)
            adapted = _FunctionHook(hook, selected)
        else:
            raise TypeError("hook must be a NodeHook or callable")

        with self._lock:
            if self._closed:
                raise RuntimeError("hook registry is closed")
            registration = _Registration(next(self._counter), adapted, graph, node)
            self._registrations = (*self._registrations, registration)
        return HookHandle(self, registration.id)

    def snapshot(self, graph: str) -> tuple[_Registration, ...]:
        """固定一次 Graph execution 可见的全部注册。

        Args:
            graph: Graph 的注册名称。

        Returns:
            全局注册与该 Graph 注册，按注册顺序排列。
        """

        with self._lock:
            return tuple(
                registration
                for registration in self._registrations
                if registration.graph is None or registration.graph == graph
            )

    @staticmethod
    def for_node(
        snapshot: tuple[_Registration, ...],
        node_id: str,
    ) -> tuple[NodeHook, ...]:
        """从 execution 快照中选择对指定节点生效的 Hook。

        Args:
            snapshot: `snapshot()` 返回的注册快照。
            node_id: Graph 内的节点 ID。

        Returns:
            按注册顺序排列的 Hook。
        """

        return tuple(
            registration.hook
            for registration in snapshot
            if registration.node is None or registration.node == node_id
        )

    def close(self) -> None:
        """停止接受新 Hook；现有句柄仍可安全 detach。"""

        with self._lock:
            self._closed = True

    def _detach(self, registration_id: int) -> None:
        """移除指定注册；编号不存在时无作用。"""

        with self._lock:
            self._registrations = tuple(
                item for item in self._registrations if item.id != registration_id
            )
