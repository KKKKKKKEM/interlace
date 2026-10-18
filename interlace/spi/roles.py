"""Runtime 与 PluginHost 依赖的角色协议；便利等待接口由门面组合 Execution 实现。

WorkerRole 由 ExecutionHost、HookHost、PolicyHost 与 ObservationHost 组合而成；装配方只依赖所需的最窄协议，
完整替代 Worker 只要结构上满足 WorkerRole 即可，不要求继承默认实现。
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from typing import Any, Protocol, runtime_checkable

from ..engine.core import Output
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.observation import ObserverHandle, RuntimeObserver
from ..engine.policies import InputSelector
from ..engine.slots import SlotProvider
from .runtime import EventHandler


@runtime_checkable
class GraphCatalog(Protocol):
    """供服务与诊断工具读取已注册图的可选角色能力。"""

    def graphs(self) -> Mapping[str, Graph]:
        """取得已冻结图的只读注册快照。

        Returns:
            注册名称到冻结 Graph 的映射。
        """
        ...


@runtime_checkable
class ObservationHost(Protocol):
    """接受只读 Runtime 生命周期观察者的角色能力。"""

    def observe_runtime(self, observer: RuntimeObserver) -> ObserverHandle:
        """注册只读运行时生命周期观察者；观察者异常不得改变业务结果。

        Args:
            observer: 接收 RuntimeEvent 的观察者。

        Returns:
            可独立卸载本次注册的句柄。
        """
        ...


@runtime_checkable
class PolicyHost(Protocol):
    """登记具名输入选择器、供后续 Graph 冻结绑定的角色能力。"""

    def register_policy(self, name: str, selector: InputSelector) -> Callable[[], None]:
        """注册带命名空间的输入选择器。

        Args:
            name: 带命名空间的选择器名称。
            selector: 仅依据端口和 token 数量选择输入的实现。

        Returns:
            仅撤销本次注册的幂等函数，已冻结 Graph 保留选择器快照。
        """
        ...


@runtime_checkable
class HookHost(Protocol):
    """挂载动态 Node Hook 和插件固定 Hook 的角色能力。"""

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """挂载动态 Node Hook，对此后开始的 execution 生效。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段；NodeHook 对象必须为 None。
            graph: 限定生效的 Graph 注册名称，None 表示全部 Graph。
            node: 限定生效的 Graph 内节点 ID，需要同时指定 graph。

        Returns:
            可独立卸载本次挂载的句柄。
        """
        ...

    def contribute_hook(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> Callable[[], None]:
        """贡献插件固定 Hook；目标 Graph 尚未注册时延迟到注册时绑定。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段；NodeHook 对象必须为 None。
            graph: 限定生效的 Graph 注册名称，None 表示全部 Graph。
            node: 限定生效的 Graph 内节点 ID，需要同时指定 graph。

        Returns:
            幂等注销函数；目标 Graph 注册前后均能撤销本次贡献。
        """
        ...


@runtime_checkable
class ExecutionHost(Protocol):
    """注册 Graph、消费队列 Work 并提供 Execution 句柄的角色能力。"""

    @property
    def idle(self) -> bool:
        """判断是否没有排队或执行中的 Work 与直接执行。

        Returns:
            没有未完成执行时为 True。
        """
        ...

    def register(self, name: str, graph: Graph) -> object:
        """冻结并注册具名 Graph，供直接执行和队列 Work 查找。

        Args:
            name: Graph 的注册名称。
            graph: 需要注册的 Graph；尚未冻结时由实现冻结。

        Returns:
            实现自行选择的组合结果，调用方不依赖其具体类型。
        """
        ...

    def consume(
        self,
        queue: str,
        *,
        concurrency: int = 1,
        slots: SlotProvider | None = None,
    ) -> Callable[[], None]:
        """为命名通道注册本地消费者与独立的执行并发限制。

        Args:
            queue: 命名消费通道。
            concurrency: 同时执行的完整 Graph execution 上限。
            slots: 为根 Work 申请 Slot 的提供方；None 时由实现自建。

        Returns:
            幂等释放函数：撤销本次调用新建的消费者绑定及其自建 SlotPool；队列此前已由相同配置消费时为空操作，
            不影响既有消费者。
        """
        ...

    def start(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        max_queued: int = 0,
        output_buffer: int = 64,
        options: Mapping[str, Any] | None = None,
    ) -> Execution:
        """提交 Graph 执行并返回统一 Execution 句柄。

        Args:
            graph: 已注册 Graph 的名称。
            inputs: 入口数据，或多入口端口名到数据的映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 整次执行的时限，单位秒；None 表示不限制。
            max_queued: Graph 内待消费输入 token 总数上限，0 表示不限制。
            output_buffer: 每个活跃输出流允许积压的输出条数。
            options: 本次执行的领域配置，提交时深复制，不随跨图事件继承。

        Returns:
            本次提交的 Execution。
        """
        ...

    def iter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        max_queued: int = 0,
        output_buffer: int = 64,
        options: Mapping[str, Any] | None = None,
    ) -> Iterator[Output]:
        """先建立输出订阅再启动 Graph，返回同步终端输出流。

        Args:
            graph: 已注册 Graph 的名称。
            inputs: 入口数据，或多入口端口名到数据的映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 整次执行的时限，单位秒；None 表示不限制。
            max_queued: Graph 内待消费输入 token 总数上限，0 表示不限制。
            output_buffer: 输出流允许积压的输出条数，满时对执行施加背压。
            options: 本次执行的领域配置，提交时深复制，不随跨图事件继承。

        Returns:
            按产生顺序交付终端 Output 的迭代器。
        """
        ...

    def aiter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        max_queued: int = 0,
        output_buffer: int = 64,
        options: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[Output]:
        """先建立输出订阅再启动 Graph，返回异步终端输出流。

        Args:
            graph: 已注册 Graph 的名称。
            inputs: 入口数据，或多入口端口名到数据的映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 整次执行的时限，单位秒；None 表示不限制。
            max_queued: Graph 内待消费输入 token 总数上限，0 表示不限制。
            output_buffer: 输出流允许积压的输出条数，满时对执行施加背压。
            options: 本次执行的领域配置，提交时深复制，不随跨图事件继承。

        Returns:
            按产生顺序交付终端 Output 的异步迭代器。
        """
        ...

    def get_execution(self, execution_id: str) -> Execution:
        """按 ID 取得仍被保留的 Execution 句柄。

        Args:
            execution_id: Execution ID；队列 Work 的 Execution ID 等于 Work.id。

        Returns:
            对应的 Execution。
        """
        ...

    def executions(self) -> tuple[Execution, ...]:
        """返回当前保留的 Execution 句柄快照。

        Returns:
            当前保留的 Execution。
        """
        ...

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的执行完成，并传播实现记录的失败。

        Args:
            timeout: 最长等待秒数；None 表示一直等待。
        """
        ...

    def close(self) -> None:
        """停止接受新执行，排空在途工作并释放拥有的资源。"""
        ...


@runtime_checkable
class WorkerRole(ExecutionHost, HookHost, PolicyHost, ObservationHost, Protocol):
    """Runtime 依赖的完整执行角色：执行、Hook、选择器与观察能力的组合，默认实现为 GraphWorker。"""


@runtime_checkable
class RouterRole(ObservationHost, Protocol):
    """Runtime 依赖的事件角色：观察、路由与发布 Event，默认实现为 EventRouter。"""

    @property
    def idle(self) -> bool:
        """判断是否没有在途 Event 分发。

        Returns:
            没有在途分发时为 True。
        """
        ...

    def observe(self, event_type: str, handler: EventHandler) -> object:
        """注册独立 Event 观察者，不建立目标 Graph 路由；观察者异常不传回 emit。

        Args:
            event_type: 精确事件类型，或匹配全部类型的 ``"*"``。
            handler: 接收 Event 的观察函数。

        Returns:
            实现自行选择的组合结果，调用方不依赖其具体类型。
        """
        ...

    def route(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        subscription: str | None = None,
        limits: ExecutionLimits | None = None,
    ) -> object:
        """将事件类型连接到目标 Graph 和命名消费通道。

        Args:
            event_type: 精确事件类型，或匹配全部类型的 ``"*"``。
            graph: 目标 Graph 的注册名称。
            queue: 接收 Work 的命名通道。
            subscription: 竞争消费组名称，None 时由实现按路由生成。
            limits: 每个 Work 独立使用的步数、时长与队列限制；None 表示不限制。

        Returns:
            实现自行选择的组合结果，调用方不依赖其具体类型。
        """
        ...

    def emit(self, event_or_type: Event | str, payload: Any = None) -> Event:
        """发布领域事件；正常返回表示事件传输已接受。

        Args:
            event_or_type: 已有 Event，或用于构造 Event 的类型字符串。
            payload: 构造 Event 时携带的领域数据；传入完整 Event 时必须为 None。

        Returns:
            已被接受的 Event。
        """
        ...

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的 Event 分发完成。

        Args:
            timeout: 最长等待秒数；None 表示一直等待。
        """
        ...

    def close(self) -> None:
        """停止接受新的路由与发布，排空在途分发并释放拥有的资源。"""
        ...
