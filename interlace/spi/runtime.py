"""Runtime 的可替换能力协议和跨后端任务模型。"""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from .._validation import require_non_empty_string
from ..engine.events import Event
from ..engine.options import snapshot_options
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.slots import Slot, SlotLease, SlotProvider

EventHandler = Callable[[Event], None]


@dataclass(frozen=True, slots=True)
class Work:
    """任务传输搬运的一次 Graph 执行请求，不携带进程内 Slot 或 lease。

    ``id`` 同时作为本地 Execution ID。同一 id 的重投递（通常 attempt 更高）在前一次 Execution 已进入终态时
    替换其保留记录，``get_execution(id)`` 返回最新一次尝试；前一次仍在执行时 Worker 以 REJECT 拒绝本次
    交付，不并发执行同一 Work。

    Attributes:
        graph: 目标 Graph 的注册名称。
        inputs: 交给目标 Graph 入口的输入数据，跨进程传输时由适配器序列化。
        trigger: 触发本 Work 的 Event；不参与相等比较和 repr。
        id: 非空唯一标识，默认生成 UUID4 字符串，适配器可传入以恢复已有 ID。
        limits: 本次执行独立使用的步数、时长与队列限制。
        options: 构造时深复制的执行配置快照，值的序列化由传输方负责。
    """

    graph: str
    inputs: Any = None
    trigger: Event | None = field(default=None, compare=False, repr=False)
    id: str = field(default_factory=lambda: str(uuid4()), kw_only=True)
    limits: ExecutionLimits = field(default_factory=ExecutionLimits, kw_only=True)
    options: Mapping[str, Any] = field(default_factory=dict, kw_only=True)

    def __post_init__(self) -> None:
        """校验 graph、id 与 limits，并固定 options 的深复制快照。

        Raises:
            TypeError: limits 不是 ExecutionLimits，或 graph、id 不是字符串。
            ValueError: graph 或 id 为空字符串。
        """

        require_non_empty_string(self.graph, "work graph")
        require_non_empty_string(self.id, "work id")
        if not isinstance(self.limits, ExecutionLimits):
            raise TypeError("work limits must be ExecutionLimits")
        object.__setattr__(self, "options", snapshot_options(self.options))


@dataclass(frozen=True, slots=True)
class Delivery:
    """TaskConsumer 交给 handler 的一次本地交付。

    重投递使用同一个 Work 和递增的 attempt；Worker 对重投递的处理规则见 Work。

    Attributes:
        work: 本次交付的 Work。
        attempt: 交付次数，首次为 1，每次重投递加 1。
        slot_lease: 随交付传递的进程内 Slot 引用，由交付方在交付结束后释放；不参与比较、repr 或序列化。
    """

    work: Work
    attempt: int = 1
    slot_lease: SlotLease | None = field(
        default=None,
        compare=False,
        repr=False,
        kw_only=True,
    )

    def __post_init__(self) -> None:
        """校验 work、attempt 与 slot_lease 的类型和取值。

        Raises:
            TypeError: work 不是 Work，或 slot_lease 不实现 SlotLease。
            ValueError: attempt 不是正整数。
        """

        if not isinstance(self.work, Work):
            raise TypeError("delivery work must be Work")
        if type(self.attempt) is not int or self.attempt < 1:
            raise ValueError("delivery attempt must be an integer greater than zero")
        if self.slot_lease is not None and not isinstance(self.slot_lease, SlotLease):
            raise TypeError("delivery slot_lease must implement SlotLease or be None")


class DeliveryOutcome(str, enum.Enum):
    """handler 对单次交付的处理决定。

    Attributes:
        ACK: 交付已完成，不再投递。
        RETRY: 请求 backend 按自身策略重新投递。
        REJECT: 拒绝本次交付，不再投递；携带的错误交给 backend 的失败处理。
    """

    ACK = "ack"
    RETRY = "retry"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """handler 返回给 backend 的唯一交付决定。

    Attributes:
        outcome: ACK、RETRY 或 REJECT。
        error: RETRY 或 REJECT 携带的原始异常；ACK 必须为 None，不参与比较和 repr。
    """

    outcome: DeliveryOutcome
    error: BaseException | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        """校验 outcome 与 error 的组合。

        Raises:
            TypeError: outcome 不是 DeliveryOutcome，或 error 不是异常实例。
            ValueError: ACK 携带了 error。
        """

        if not isinstance(self.outcome, DeliveryOutcome):
            raise TypeError("delivery outcome must be DeliveryOutcome")
        if self.error is not None and not isinstance(self.error, BaseException):
            raise TypeError("delivery error must be a BaseException or None")
        if self.outcome is DeliveryOutcome.ACK and self.error is not None:
            raise ValueError("ACK delivery result must not contain an error")

    @classmethod
    def ack(cls) -> DeliveryResult:
        """创建 ACK 决定。

        Returns:
            不携带错误的 ACK 结果。
        """

        return cls(DeliveryOutcome.ACK)

    @classmethod
    def retry(cls, error: BaseException | None = None) -> DeliveryResult:
        """创建请求重新投递的决定。

        Args:
            error: 本次失败的原因；重投递耗尽时 backend 可将其作为最终失败。

        Returns:
            RETRY 结果。
        """

        return cls(DeliveryOutcome.RETRY, error)

    @classmethod
    def reject(cls, error: BaseException | None = None) -> DeliveryResult:
        """创建拒绝本次交付的决定。

        Args:
            error: 拒绝原因；None 表示无需上报的拒绝。

        Returns:
            REJECT 结果。
        """

        return cls(DeliveryOutcome.REJECT, error)


class EventBus(Protocol):
    """Event 发布、订阅和投递生命周期的替换协议，不接触 Slot 或 lease。"""

    @property
    def idle(self) -> bool:
        """判断是否没有尚未投递完成的 Event。

        Returns:
            没有在途投递时为 True。
        """
        ...

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        *,
        subscription: str | None = None,
    ) -> Callable[[], None]:
        """订阅 Event；同名 subscription 的注册竞争消费，不同 subscription 各收到一份。

        Args:
            event_type: 精确事件类型，或匹配全部类型的 ``"*"``。
            handler: 接收 Event 的处理函数。
            subscription: 竞争消费组名称，None 创建独立订阅。

        Returns:
            仅注销本次注册的幂等函数；已经选中的投递仍可完成。
        """

    def publish(self, event: Event) -> None:
        """发布一个 Event；正常返回表示传输已接受该 Event。

        Args:
            event: 需要发布的 Event。
        """

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的 Event 投递完成。

        Args:
            timeout: 最长等待秒数；None 表示一直等待。
        """

    def close(self) -> None:
        """停止接受新的订阅和发布，并释放传输资源。"""


WorkHandler = Callable[[Delivery], DeliveryResult]


class TaskPublisher(Protocol):
    """向命名任务通道投递 Work 的发布端协议。"""

    def submit(self, queue: str, work: Work) -> None:
        """向命名通道提交 Work；正常返回表示后端已接受。

        Args:
            queue: 命名消费通道。
            work: 需要执行的 Work。
        """

    def close(self) -> None:
        """关闭发布端持有的资源。"""


@runtime_checkable
class LocalTaskPublisher(TaskPublisher, Protocol):
    """可在当前进程延续 Slot 链的可选任务发布能力，按结构化协议识别。"""

    def submit_local(self, queue: str, work: Work, lease: SlotLease) -> None:
        """提交延续 lease 所属 Slot 链的 Work。

        正常返回时接管调用方转交的一个 lease 引用；抛错时引用仍由调用方释放，接收方不得为同一交接
        再次 retain。

        Args:
            queue: 命名消费通道。
            work: 需要执行的 Work。
            lease: 当前进程内的 Slot 引用，不跨进程序列化。
        """


class TaskConsumer(Protocol):
    """从命名任务通道消费 Work，并控制本地并发与 Slot 分配的消费端协议。"""

    @property
    def idle(self) -> bool:
        """判断是否没有尚未完成的交付。

        Returns:
            没有排队或在途交付时为 True。
        """
        ...

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
        slots: SlotProvider | None = None,
    ) -> Callable[[], None]:
        """绑定通道，并为不携带 Slot 的根 Work 分配执行槽。

        Args:
            queue: 命名消费通道。
            handler: 处理 Delivery 并返回 DeliveryResult 的函数。
            concurrency: 本次绑定同时执行的 Delivery 上限。
            slots: 为根 Work 申请 Slot 的提供方；None 时由实现自建。

        Returns:
            注销本次绑定并等待已分配交付完成 lease 清理的幂等函数；排队工作遵循后端声明的排空契约。
        """

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的 Work 完成，并传播实现记录的交付失败。

        Args:
            timeout: 最长等待秒数；None 表示一直等待。
        """

    def close(self) -> None:
        """关闭消费端持有的资源。"""


class TaskBackend(TaskPublisher, TaskConsumer, Protocol):
    """同时实现发布与消费的组合协议。"""


Emit = Callable[[Event], None]


@runtime_checkable
class GraphExecutor(Protocol):
    """执行单张冻结 Graph 的替换协议，不管理队列、Graph 注册表或 Execution 终态。"""

    def execute(
        self,
        name: str,
        graph: Graph,
        inputs: Any,
        emit: Emit,
        plan: ExecutionPlan | None = None,
        *,
        slot: Slot | None = None,
        options: Mapping[str, Any] | None = None,
        execution: Execution,
    ) -> Awaitable[None] | None:
        """执行冻结 Graph，通过 execution.publish_output/apublish_output 交付结果。

        Args:
            name: Graph 的注册名称，用于观测与错误上下文。
            graph: 冻结的目标图；执行器每次节点调用使用 graph.context_factory 创建上下文。
            inputs: 已按入口端口名组织的初始输入映射。
            emit: 发布跨图事件的回调。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            slot: 当前逻辑执行链使用的本地执行槽。
            options: 本次执行的配置快照，执行器必须传入各节点的 Context。
            execution: 记录当前执行状态、控制限制及输出的句柄。

        Returns:
            同步实现返回 None；异步实现返回最终结果为 None 的等待对象。
        """

    def close(self) -> None:
        """关闭执行器持有的资源。"""


@runtime_checkable
class HookableGraphExecutor(GraphExecutor, Protocol):
    """额外支持动态 Node Hook 的 GraphExecutor 可选能力。"""

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """挂载 Hook，并返回可用于卸载的句柄。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段；NodeHook 对象必须为 None。
            graph: 限定生效的 Graph 注册名称，None 表示全部 Graph。
            node: 限定生效的 Graph 内节点 ID，需要同时指定 graph。

        Returns:
            可独立卸载本次挂载的句柄。
        """
        ...


class ExecutionFactory(Protocol):
    """为直接执行和队列 Work 创建 Execution 句柄的工厂协议，可注入 OutputStore 与 ExecutionNotifier。"""

    def __call__(
        self,
        graph: str,
        *,
        limits: ExecutionLimits,
        id: str | None = None,
        output_buffer: int = 64,
    ) -> Execution:
        """创建一个尚未开始的全新 Execution。

        Args:
            graph: 目标 Graph 的注册名称。
            limits: 本次执行独立使用的步数、时长与队列限制。
            id: Execution ID；队列 Work 传入 Work.id，None 时由实现生成。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            处于未开始状态、由宿主调用 start/succeed/fail 的 Execution。
        """
        ...
