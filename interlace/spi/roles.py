"""Runtime 依赖的角色协议；便利等待接口由门面组合 Execution 实现。

Router 与 Worker 按结构化协议替换，不要求继承默认实现；扩展行为经 Runtime 的钩子总线，不属于角色协议。
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from typing import Any, Protocol, runtime_checkable

from ..engine.core import Output
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.slots import SlotLease, SlotProvider
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
        slots: SlotProvider | int | None = None,
    ) -> Callable[[], None]:
        """为命名通道注册本地消费者与独立的执行并发限制。

        Args:
            queue: 命名消费通道。
            concurrency: 同时执行的完整 Graph execution 上限。
            slots: 为根 Work 申请 Slot 的提供方；整数表示自建该容量的池，None 时由实现按 concurrency 自建。

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
        slots: SlotProvider | None = None,
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
            slots: 为本次执行申请 Slot 的提供方；None 时不使用 Slot。给定时等待 Slot 不占用执行线程，
                期间取消结束为 CANCELLED，申请失败结束为 FAILED，整次执行在该 Slot 内串行运行。

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
        slots: SlotProvider | None = None,
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
            slots: 为本次执行申请 Slot 的提供方；None 时不使用 Slot。给定时等待 Slot 不占用执行线程，
                期间取消结束为 CANCELLED，申请失败结束为 FAILED，整次执行在该 Slot 内串行运行。

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
        slots: SlotProvider | None = None,
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
            slots: 为本次执行申请 Slot 的提供方；None 时不使用 Slot。给定时等待 Slot 不占用执行线程，
                期间取消结束为 CANCELLED，申请失败结束为 FAILED，整次执行在该 Slot 内串行运行。

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

    def begin_close(self) -> None:
        """开始关闭前的预备：拒绝新的直接执行，解除未结束执行的输出背压，不等待。"""
        ...

    def close(self) -> None:
        """停止接受新执行，排空在途工作并释放拥有的资源。"""
        ...


@runtime_checkable
class WorkerRole(ExecutionHost, Protocol):
    """Runtime 依赖的执行角色，默认实现为 GraphWorker。"""


@runtime_checkable
class RouterRole(Protocol):
    """Runtime 依赖的事件角色：观察、路由与发布 Event，默认实现为 EventRouter。"""

    @property
    def idle(self) -> bool:
        """判断是否没有在途 Event 分发。

        Returns:
            没有在途分发时为 True。
        """
        ...

    def observe(self, event_type: str, handler: EventHandler) -> Callable[[], None]:
        """注册独立 Event 观察者，不建立目标 Graph 路由；观察者异常不传回 emit。

        Args:
            event_type: 精确事件类型，或匹配全部类型的 ``"*"``。
            handler: 接收 Event 的观察函数。

        Returns:
            撤销本次观察的幂等函数。
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
    ) -> Callable[[], None]:
        """将事件类型连接到目标 Graph 和命名消费通道。

        Args:
            event_type: 精确事件类型，或匹配全部类型的 ``"*"``。
            graph: 目标 Graph 的注册名称。
            queue: 接收 Work 的命名通道。
            subscription: 竞争消费组名称，None 时由实现按路由生成。
            limits: 每个 Work 独立使用的步数、时长与队列限制；None 表示不限制。

        Returns:
            撤销本次路由的幂等函数。
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

    def publish(self, event: Event) -> object:
        """发布 Worker 产生的 Event，路由产生的 Work 开始新的 Slot 链。

        Args:
            event: 需要发布的 Event。

        Returns:
            实现自行选择的结果，调用方不依赖其具体类型。
        """
        ...

    def publish_local(self, event: Event, lease: SlotLease) -> object:
        """发布 Event，并让本次同步分发产生的路由 Work 延续 lease 所属的 Slot 链。

        Args:
            event: 需要发布的 Event。
            lease: 源 execution 持有的 Slot lease。

        Returns:
            实现自行选择的结果，调用方不依赖其具体类型。
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
