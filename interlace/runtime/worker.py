"""GraphWorker：Graph 注册、Work 消费与 execution 宿主编排。"""

from __future__ import annotations

import inspect
import time
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from dataclasses import dataclass
from functools import partial
from threading import Lock, RLock
from types import MappingProxyType, TracebackType
from typing import Any

from .._validation import require_non_empty_string, validate_timeout
from ..engine.core import Output
from ..engine.errors import ExecutionError, InterlaceRuntimeError, UnknownGraphError
from ..engine.events import Event
from ..engine.execution import (
    Execution,
    ExecutionLimits,
    ExecutionStatus,
    terminal_status,
)
from ..engine.executor import Engine
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.observation import (
    ObservationHub,
    ObserverHandle,
    RuntimeEvent,
    RuntimeEventKind,
    RuntimeObserver,
)
from ..engine.options import snapshot_options
from ..engine.policies import InputSelector, PolicyRegistry
from ..engine.runner import LocalRunner
from ..engine.slots import Slot, SlotPool
from ..spi import (
    Delivery,
    DeliveryResult,
    Emit,
    ExecutionFactory,
    GraphExecutor,
    HookableGraphExecutor,
    SlotLease,
    SlotProvider,
    TaskConsumer,
    Work,
)
from ._utils import _close_components, _Lifecycle, _remaining, _unique
from .contributions import _DeferredHooks, _HookContribution
from .direct import _DirectRunner
from .history import _ExecutionHistory


@dataclass(frozen=True, slots=True)
class _QueueBinding:
    """Worker 在一个命名通道上的消费者绑定。

    Attributes:
        concurrency: 该通道同时运行的完整 Graph execution 上限。
        slots: 根 Work 申请 Slot 的资源池。
        unbind: TaskConsumer.bind 返回的幂等注销函数。
        owned_slots: Worker 自建并负责关闭的默认 SlotPool；调用方提供 slots 时为 None。
    """

    concurrency: int
    slots: SlotProvider
    unbind: Callable[[], None]
    owned_slots: SlotPool | None


class GraphWorker:
    """注册 Graph、消费队列 Work，并作为执行宿主运行完整 Graph。

    生命周期为 OPEN -> CLOSING -> CLOSED。首个 `close()` 进入 CLOSING：拒绝新的直接执行和配置变更，
    已绑定通道中的 Work 及其延续继续排空；排空后注销消费者、释放自有组件并进入 CLOSED。并发的
    `close()` 等待同一次关闭完成。

    Attributes:
        _consumer: 绑定命名通道并交付 Work 的任务消费能力。
        _executor: 执行冻结 Graph 的实现。
        _execution_factory: 创建 Execution 句柄的工厂。
        _executor_runner: 等待异步 GraphExecutor 结果的运行器，首次需要时创建。
        _emit: 跨图事件发布回调。
        _emit_local: 携带 Slot lease 发布跨图事件的回调，None 时退化为 _emit。
        _owned_components: Worker 自建并负责关闭的组件。
        _close_injected: 关闭时是否同时关闭注入的 consumer 与 executor。
        _observations: Worker 生命周期事件的分发中心。
        _policies: 冻结 Graph 时使用的输入策略注册表。
        _graphs: 按注册名保存的冻结 Graph，只增不减。
        _queues: 按通道名保存的消费者绑定，受 _configure_lock 保护。
        _hooks: 等待目标 Graph 注册的插件 Hook 贡献，受 _lock 保护。
        _history: 本地 Execution 记录与活跃计数。
        _direct: 直接执行线程池。
        _lifecycle: OPEN/CLOSING/CLOSED 关闭状态。
        _lock: 保护 Graph 注册表、延迟 Hook 与运行器创建；持有期间不调用任务后端。
        _configure_lock: 串行化消费者绑定与注销，使后端调用不占用 _lock。
    """

    def __init__(
        self,
        *,
        consumer: TaskConsumer,
        executor: GraphExecutor | None = None,
        execution_factory: ExecutionFactory | None = None,
        emit: Emit | None = None,
        emit_local: Callable[[Event, SlotLease], None] | None = None,
        close_injected: bool = False,
        observations: ObservationHub | None = None,
        policies: PolicyRegistry | None = None,
        history_limit: int = 1000,
        direct_workers: int | None = None,
    ) -> None:
        """装配任务消费、Graph 执行与本地 Execution 记录。

        Args:
            consumer: 绑定命名通道并交付 Work 的任务消费能力。
            executor: Graph 执行器；None 时创建并拥有默认 Engine。
            execution_factory: 创建 Execution 句柄的工厂；None 使用 Execution。
            emit: 发布跨图事件的回调；None 时 Node 发出事件会失败。
            emit_local: 携带当前 Slot lease 发布跨图事件的回调；None 时使用 emit，不延续 Slot 链。
            close_injected: 关闭时是否同时关闭注入的 consumer 与 executor。
            observations: 生命周期事件分发中心；None 时新建。
            policies: 冻结 Graph 时使用的输入策略注册表；None 时新建。
            history_limit: 保留 Execution 记录的数量上限，超出后淘汰最早的已结束记录；默认 1000。
            direct_workers: `start/iter/aiter` 直接执行线程池的线程数上限；None 使用标准库默认值。

        Raises:
            TypeError: 参数类型不符合契约。
            ValueError: history_limit 或 direct_workers 小于 1。
        """

        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        if execution_factory is not None and not callable(execution_factory):
            raise TypeError("execution_factory must be callable")
        if executor is not None and not isinstance(executor, GraphExecutor):
            raise TypeError("executor must implement GraphExecutor")
        if emit is not None and not callable(emit):
            raise TypeError("worker emitter must be callable")
        if emit_local is not None and not callable(emit_local):
            raise TypeError("worker local emitter must be callable or None")
        self._history = _ExecutionHistory(history_limit)
        self._direct = _DirectRunner(direct_workers)
        owned: list[object] = []
        if observations is None:
            observations = ObservationHub()
        if executor is None:
            executor = Engine(observations=observations)
            owned.append(executor)
        self._consumer = consumer
        self._executor = executor
        self._execution_factory = (
            Execution if execution_factory is None else execution_factory
        )
        self._executor_runner: LocalRunner | None = None
        self._emit = _reject_emit if emit is None else emit
        self._emit_local = emit_local
        self._owned_components = owned
        self._close_injected = close_injected
        self._observations = observations
        self._policies = PolicyRegistry() if policies is None else policies
        self._graphs: dict[str, Graph] = {}
        self._queues: dict[str, _QueueBinding] = {}
        self._hooks = _DeferredHooks(self._install_hook)
        self._lifecycle = _Lifecycle("GraphWorker")
        self._lock = RLock()
        self._configure_lock = Lock()

    @property
    def idle(self) -> bool:
        """返回是否没有排队、执行中或尚未收尾的 Work 与直接执行。

        Returns:
            任务消费者空闲且没有活跃 Execution 时为 True。
        """

        return self._history.idle and self._consumer.idle

    def register(self, name: str, graph: Graph) -> GraphWorker:
        """以稳定名称注册 Graph，必要时用 Worker 的策略注册表冻结它，并安装延迟 Hook。

        Args:
            name: 非空注册名，同一 Worker 内唯一。
            graph: 待注册的 Graph；未冻结时就地冻结。

        Returns:
            当前 Worker，便于链式注册。

        Raises:
            TypeError: graph 不是 Graph。
            ValueError: 名称为空，或延迟 Hook 指向 Graph 中不存在的节点。
            InterlaceRuntimeError: 名称已经注册。
            RuntimeClosedError: Worker 已开始关闭。
        """

        name = require_non_empty_string(name, "registered graph name")
        if not isinstance(graph, Graph):
            raise TypeError("graph must be a Graph")
        with self._lock:
            self._lifecycle.ensure_open()
            if name in self._graphs:
                raise InterlaceRuntimeError(f"duplicate registered graph {name!r}")
            if not graph.frozen:
                graph.freeze(self._policies)
            self._hooks.bind(name, graph)
            self._graphs[name] = graph
        return self

    def graphs(self) -> Mapping[str, Graph]:
        """取得供服务与诊断使用的冻结图注册快照。

        Returns:
            注册名到冻结 Graph 的只读映射，不随后续注册变化。
        """

        with self._lock:
            return MappingProxyType(dict(self._graphs))

    def consume(
        self,
        queue: str,
        *,
        concurrency: int = 1,
        slots: SlotProvider | None = None,
    ) -> Callable[[], None]:
        """绑定命名通道的本地消费者；concurrency 限制 Graph execution，slots 限制逻辑执行链。

        同一通道以相同配置重复消费时不再绑定。

        Args:
            queue: 非空命名消费通道。
            concurrency: 该通道同时运行的完整 Graph execution 上限，至少为 1。
            slots: 根 Work 申请 Slot 的资源池；None 时创建大小等于 concurrency 的自有 SlotPool。

        Returns:
            幂等释放函数：撤销本次新建的消费者绑定及其自建 SlotPool；通道此前已由相同配置消费时为空操作。

        Raises:
            TypeError: concurrency 不是整数，或 slots 未实现 SlotProvider。
            ValueError: queue 为空或 concurrency 小于 1。
            InterlaceRuntimeError: 通道已使用不同的并发或 SlotPool 消费。
            RuntimeClosedError: Worker 已开始关闭。
        """

        queue = require_non_empty_string(queue, "task queue")
        if type(concurrency) is not int:
            raise TypeError("queue concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("queue concurrency must be at least 1")
        if slots is not None and not isinstance(slots, SlotProvider):
            raise TypeError("slots must implement SlotProvider or be None")
        with self._configure_lock:
            self._lifecycle.ensure_open()
            configured = self._queues.get(queue)
            if configured is not None:
                if configured.concurrency != concurrency:
                    raise InterlaceRuntimeError(
                        f"queue {queue!r} already uses local concurrency "
                        f"{configured.concurrency}"
                    )
                if slots is not None and slots is not configured.slots:
                    raise InterlaceRuntimeError(
                        f"queue {queue!r} already uses a different SlotPool"
                    )
                return _noop
            owned_slots = None
            if slots is None:
                slots = owned_slots = SlotPool(concurrency)
            try:
                unbind = self._consumer.bind(
                    queue,
                    self._execute_delivery,
                    concurrency=concurrency,
                    slots=slots,
                )
            except BaseException:
                if owned_slots is not None:
                    owned_slots.close()
                raise
            binding = _QueueBinding(concurrency, slots, unbind, owned_slots)
            self._queues[queue] = binding
        return partial(self._release_queue, queue, binding)

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
        """在直接执行线程池中启动已注册 Graph，并立即返回 Execution。

        Args:
            graph: 已注册 Graph 的名称。
            inputs: 入口数据，或多入口端口名到数据的映射。
            plan: 限定本次执行范围的计划，None 执行完整 Graph。
            max_steps: Node firing 次数上限，0 表示不限制。
            timeout: Graph execution 时限，单位秒；None 表示不限时。
            max_queued: Graph 内待消费输入 token 总数上限，0 表示不限制。
            output_buffer: 每个活跃输出流允许积压的 Output 条数。
            options: 本次执行的领域配置，提交前深复制，不随跨图事件继承。

        Returns:
            可等待、迭代和取消的 Execution。

        Raises:
            UnknownGraphError: graph 未注册。
            RuntimeClosedError: Worker 已开始关闭。
        """

        prepared_options = snapshot_options(options)
        execution = self._new_execution(
            graph,
            max_steps=max_steps,
            timeout=timeout,
            max_queued=max_queued,
            output_buffer=output_buffer,
        )
        self._submit_direct(execution, inputs, plan, prepared_options)
        return execution

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
        """启动 Graph，并返回在提交前已订阅的同步 terminal Output 流。

        Args:
            graph: 已注册 Graph 的名称。
            inputs: 入口数据，或多入口端口名到数据的映射。
            plan: 限定本次执行范围的计划，None 执行完整 Graph。
            max_steps: Node firing 次数上限，0 表示不限制。
            timeout: Graph execution 时限，单位秒；None 表示不限时。
            max_queued: Graph 内待消费输入 token 总数上限，0 表示不限制。
            output_buffer: 每个活跃输出流允许积压的 Output 条数。
            options: 本次执行的领域配置，提交前深复制，不随跨图事件继承。

        Returns:
            按产生顺序交付 terminal Output 的迭代器。

        Raises:
            UnknownGraphError: graph 未注册。
            RuntimeClosedError: Worker 已开始关闭。
        """

        prepared_options = snapshot_options(options)
        execution = self._new_execution(
            graph,
            max_steps=max_steps,
            timeout=timeout,
            max_queued=max_queued,
            output_buffer=output_buffer,
        )
        stream = iter(execution)
        self._submit_direct(execution, inputs, plan, prepared_options)
        return stream

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
        """启动 Graph，并返回在提交前已订阅的异步 terminal Output 流。

        Args:
            graph: 已注册 Graph 的名称。
            inputs: 入口数据，或多入口端口名到数据的映射。
            plan: 限定本次执行范围的计划，None 执行完整 Graph。
            max_steps: Node firing 次数上限，0 表示不限制。
            timeout: Graph execution 时限，单位秒；None 表示不限时。
            max_queued: Graph 内待消费输入 token 总数上限，0 表示不限制。
            output_buffer: 每个活跃输出流允许积压的 Output 条数。
            options: 本次执行的领域配置，提交前深复制，不随跨图事件继承。

        Returns:
            按产生顺序交付 terminal Output 的异步迭代器。

        Raises:
            UnknownGraphError: graph 未注册。
            RuntimeClosedError: Worker 已开始关闭。
        """

        prepared_options = snapshot_options(options)
        execution = self._new_execution(
            graph,
            max_steps=max_steps,
            timeout=timeout,
            max_queued=max_queued,
            output_buffer=output_buffer,
        )
        stream = execution.__aiter__()
        self._submit_direct(execution, inputs, plan, prepared_options)
        return stream

    def get_execution(self, execution_id: str) -> Execution:
        """按 ID 返回当前进程保留的 Execution。

        队列 Work 的执行 ID 等于 `Work.id`；同一 Work 重投递后返回最新一次尝试。

        Args:
            execution_id: 非空执行 ID。

        Returns:
            保留的 Execution 句柄。

        Raises:
            InterlaceRuntimeError: 没有保留该 ID 的执行，或记录已被历史上限淘汰。
        """

        execution_id = require_non_empty_string(execution_id, "execution id")
        return self._history.get(execution_id)

    def executions(self) -> tuple[Execution, ...]:
        """按登记顺序返回当前进程保留的 Execution 快照。

        Returns:
            保留的 Execution 句柄，最多 history_limit 项外加仍未结束的执行。
        """

        return self._history.snapshot()

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """动态挂载 Node Hook；变更从下一次 Graph execution 生效。

        Args:
            hook: NodeHook 对象，或配合 phase 使用的单阶段回调。
            phase: 单阶段回调对应的阶段，Hook 对象为 None。
            graph: 目标 Graph 注册名，None 作用于全部 Graph；必须已经注册。
            node: 目标节点 ID，None 作用于整张 Graph；指定时必须同时指定 graph。

        Returns:
            卸载本次注册的句柄。

        Raises:
            UnknownGraphError: graph 未注册。
            ValueError: graph 中不存在 node。
            TypeError: 当前 GraphExecutor 不支持 Hook。
            RuntimeClosedError: Worker 已开始关闭。
        """

        self._lifecycle.ensure_open()
        if graph is not None:
            graph = require_non_empty_string(graph, "hook graph")
            registered = self._get_graph(graph)
            if node is not None:
                node = require_non_empty_string(node, "hook node")
                if node not in registered.nodes:
                    raise ValueError(f"graph {graph!r} has no node {node!r}")
        return self._attach_executor_hook(hook, phase=phase, graph=graph, node=node)

    def contribute_hook(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> Callable[[], None]:
        """安装插件 Hook；目标 Graph 尚未注册时延迟到注册时安装。

        Args:
            hook: NodeHook 对象，或配合 phase 使用的单阶段回调。
            phase: 单阶段回调对应的阶段，Hook 对象为 None。
            graph: 目标 Graph 注册名，None 作用于全部 Graph；可以尚未注册。
            node: 目标节点 ID，None 作用于整张 Graph；指定时必须同时指定 graph。

        Returns:
            幂等注销函数，目标 Graph 注册前后均可撤销本次贡献。

        Raises:
            ValueError: 只指定 node 未指定 graph，或已注册 graph 中不存在 node。
            TypeError: 当前 GraphExecutor 不支持 Hook。
            RuntimeClosedError: Worker 已开始关闭。
        """

        self._lifecycle.ensure_open()
        if graph is None and node is not None:
            raise ValueError("node-scoped hook requires graph")
        if graph is not None:
            graph = require_non_empty_string(graph, "hook graph")
        if node is not None:
            node = require_non_empty_string(node, "hook node")
        if not isinstance(self._executor, HookableGraphExecutor):
            raise TypeError("the configured GraphExecutor does not support hooks")
        contribution = _HookContribution(hook, phase, graph, node)
        with self._lock:
            registered = None if graph is None else self._graphs.get(graph)
            if graph is not None and registered is None:
                self._hooks.defer(contribution)
            else:
                if (
                    registered is not None
                    and node is not None
                    and node not in registered.nodes
                ):
                    raise ValueError(f"graph {graph!r} has no node {node!r}")
                self._hooks.install(contribution)
        return partial(self._withdraw_hook, contribution)

    def register_policy(self, name: str, selector: InputSelector) -> Callable[[], None]:
        """注册 selector contribution；使用它的 Graph 必须尚未冻结。

        Args:
            name: 带命名空间的策略名。
            selector: 只依据端口和 token 数量选择输入的实现。

        Returns:
            仅撤销本次策略注册的幂等函数，不影响已冻结的 Graph。

        Raises:
            RuntimeClosedError: Worker 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return self._policies.register(name, selector)

    def observe_runtime(self, observer: RuntimeObserver) -> ObserverHandle:
        """注册只读运行时生命周期观察者。

        Args:
            observer: 接收 Worker 发布的 RuntimeEvent 的观察者。

        Returns:
            卸载本次注册的句柄。
        """

        return self._observations.attach(observer)

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的 Work 与直接执行全部收尾，并传播任务消费者记录的失败。

        Args:
            timeout: 最长等待秒数，None 表示不限时。

        Raises:
            TimeoutError: 未在时限内空闲。
        """

        validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        failure: BaseException | None = None
        while True:
            try:
                self._consumer.wait_idle(_remaining(deadline))
            except BaseException as exc:
                if not self._consumer.idle:
                    raise
                if failure is None:
                    failure = exc
            if not self._history.wait_idle(_remaining(deadline)):
                # 已从消费者取出的失败不会再次上报，超时前优先传播，避免静默丢失。
                if failure is not None:
                    raise failure
                raise TimeoutError("GraphWorker did not become idle")
            if self.idle:
                if failure is not None:
                    raise failure
                return

    def close(self) -> None:
        """进入 CLOSING 排空已接受工作，随后注销消费者并关闭自有组件。

        并发调用等待同一次关闭完成后返回；关闭完成后重复调用无效果。

        Raises:
            Exception: 排空或关闭组件时最先出现的失败；其余组件仍会关闭。
        """

        if not self._lifecycle.begin_close():
            return
        # 未读取的 iter/aiter 订阅不能阻止排空；剩余输出留在存储中，订阅方之后仍可完整读取。
        self._history.release_backpressure()
        try:
            failure = self._drain_and_release()
        finally:
            self._lifecycle.finish_close()
        if failure is not None:
            raise failure

    def __enter__(self) -> GraphWorker:
        """进入资源作用域。

        Returns:
            当前 Worker。

        Raises:
            RuntimeClosedError: Worker 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """关闭 Worker；作用域内已有异常时不以关闭失败覆盖它。"""

        del exc_type, traceback
        try:
            self.close()
        except Exception:
            if exc_value is None:
                raise

    def _drain_and_release(self) -> BaseException | None:
        """等待空闲后依次关闭直接执行池、消费者绑定、组件、异步运行器和自有 SlotPool。

        Returns:
            最先出现的失败；全部成功时为 None。
        """

        failure: BaseException | None = None
        try:
            self.wait_idle()
        except BaseException as exc:
            # 控制异常同样先记录，确保后续组件全部关闭后再重新抛出。
            failure = exc
        try:
            self._direct.close()
        except BaseException as exc:
            if failure is None:
                failure = exc
        with self._configure_lock:
            bindings = tuple(reversed(self._queues.values()))
            self._queues.clear()
            for binding in bindings:
                try:
                    binding.unbind()
                except BaseException as exc:
                    if failure is None:
                        failure = exc
        components = (
            _unique(self._consumer, self._executor)
            if self._close_injected
            else tuple(self._owned_components)
        )
        failure = _close_components(reversed(components), failure)
        with self._lock:
            runner = self._executor_runner
        if runner is not None:
            failure = _close_components((runner,), failure)
        owned_slots = tuple(
            binding.owned_slots
            for binding in bindings
            if binding.owned_slots is not None
        )
        return _close_components(owned_slots, failure)

    def _release_queue(self, queue: str, binding: _QueueBinding) -> None:
        """binding 仍是通道当前绑定时注销消费者并关闭其自有 SlotPool；重复调用无效果。"""

        with self._configure_lock:
            if self._queues.get(queue) is not binding:
                return
            del self._queues[queue]
            try:
                binding.unbind()
            finally:
                if binding.owned_slots is not None:
                    binding.owned_slots.close()

    def _execute_delivery(self, delivery: Delivery) -> DeliveryResult:
        """执行一次交付并发布 work.finished；成功 ACK，任何异常 REJECT。"""

        work = delivery.work
        try:
            self._execute_work(work, delivery.slot_lease)
        except BaseException as exc:
            self._observations.publish(
                RuntimeEvent(
                    RuntimeEventKind.WORK_FINISHED,
                    graph=work.graph,
                    execution_id=work.id,
                    work_id=work.id,
                    status="rejected",
                    error_type=type(exc).__name__,
                    attributes={"attempt": delivery.attempt},
                )
            )
            return DeliveryResult.reject(exc)
        self._observations.publish(
            RuntimeEvent(
                RuntimeEventKind.WORK_FINISHED,
                graph=work.graph,
                execution_id=work.id,
                work_id=work.id,
                status="acked",
                attributes={"attempt": delivery.attempt},
            )
        )
        return DeliveryResult.ack()

    def _execute_work(self, work: Work, lease: SlotLease | None) -> None:
        """以 Work.id 登记 Execution，并在 Slot lease 的串行区间内执行目标 Graph。

        Raises:
            InterlaceRuntimeError: 同一 Work.id 的前一次执行仍未结束，或交付未携带 Slot lease。
            UnknownGraphError: 目标 Graph 未注册。
            BaseException: Graph execution 的失败，ExecutionError 会补充触发 Event。
        """

        execution = self._make_execution(work.graph, limits=work.limits, id=work.id)
        self._history.record(execution)
        try:
            graph = self._get_graph(work.graph)
            if lease is None:
                raise InterlaceRuntimeError(
                    "TaskConsumer dispatched Work without a Slot"
                )
            with lease.execution() as slot:
                self._execute(
                    execution,
                    graph,
                    work.inputs,
                    partial(self._emit_with_lease, lease),
                    slot=slot,
                    options=work.options,
                )
        except BaseException as exc:
            execution.fail(exc)
            if isinstance(exc, ExecutionError) and exc.event is None:
                exc.event = work.trigger
            raise
        finally:
            self._history.finish(execution)

    def _new_execution(
        self,
        graph: str,
        *,
        max_steps: int,
        timeout: float | None,
        max_queued: int,
        output_buffer: int,
    ) -> Execution:
        """为直接执行创建并登记 Execution。

        Raises:
            RuntimeClosedError: Worker 已开始关闭。
            UnknownGraphError: graph 未注册。
        """

        self._lifecycle.ensure_open()
        self._get_graph(graph)
        execution = self._make_execution(
            graph,
            limits=ExecutionLimits(max_steps, timeout, max_queued),
            output_buffer=output_buffer,
        )
        self._history.record(execution)
        return execution

    def _make_execution(
        self,
        graph: str,
        *,
        limits: ExecutionLimits,
        id: str | None = None,
        output_buffer: int = 64,
    ) -> Execution:
        """通过执行工厂创建 PENDING 句柄，并校验工厂未改写身份、限制与缓冲参数。

        Raises:
            TypeError: 工厂返回的不是 Execution。
            ValueError: 返回句柄与请求参数不一致或不处于 PENDING。
        """

        execution = self._execution_factory(
            graph,
            limits=limits,
            id=id,
            output_buffer=output_buffer,
        )
        if not isinstance(execution, Execution):
            raise TypeError("execution_factory must return Execution")
        if (
            execution.graph != graph
            or execution.limits != limits
            or execution.output_buffer != output_buffer
            or (id is not None and execution.id != id)
            or execution.status is not ExecutionStatus.PENDING
        ):
            raise ValueError("execution_factory changed the execution contract")
        return execution

    def _submit_direct(
        self,
        execution: Execution,
        inputs: Any,
        plan: ExecutionPlan | None,
        options: Mapping[str, Any],
    ) -> None:
        """把已登记的 Execution 交给直接执行线程池；提交失败时把失败写入 Execution 并结束登记。

        Raises:
            RuntimeClosedError: 直接执行线程池已经关闭。
        """

        try:
            self._direct.submit(
                partial(self._run_direct, execution, inputs, plan, options)
            )
        except BaseException as exc:
            execution.fail(exc)
            self._history.finish(execution)
            raise

    def _run_direct(
        self,
        execution: Execution,
        inputs: Any,
        plan: ExecutionPlan | None,
        options: Mapping[str, Any],
    ) -> None:
        """在直接执行线程中运行 Graph；失败只写入 Execution，不向线程池传播。"""

        try:
            self._execute(
                execution,
                self._get_graph(execution.graph),
                inputs,
                self._emit,
                plan=plan,
                options=options,
            )
        except BaseException as exc:
            execution.fail(exc)
        finally:
            self._history.finish(execution)

    def _execute(
        self,
        execution: Execution,
        graph: Graph,
        inputs: Any,
        emit: Emit,
        *,
        plan: ExecutionPlan | None = None,
        slot: Slot | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> None:
        """作为执行宿主调用 GraphExecutor，发布生命周期事件并写入 Execution 终态。

        execution.finished 在 `succeed()`/`fail()` 唤醒等待方之前发布，等待返回时观察者已收到终态事件。
        执行开始前已取消的 Execution 直接返回，不发布事件。

        Args:
            execution: 本次执行的 PENDING 句柄。
            graph: execution.graph 对应的冻结 Graph。
            inputs: 入口数据，或多入口端口名到数据的映射。
            emit: Node 发出跨图事件时使用的回调。
            plan: 限定执行范围的计划，必须属于 graph；None 执行完整 Graph。
            slot: 当前逻辑执行链的 Slot；直接执行为 None。
            options: 本次执行配置的快照。

        Raises:
            TypeError: plan 不是 ExecutionPlan，或执行器返回了非 None 结果。
            ValueError: plan 属于其他 Graph。
            BaseException: 执行器、取消、超时或步数限制的失败，已写入 Execution。
        """

        if not execution.start(graph):
            return
        self._observations.publish(
            RuntimeEvent(
                RuntimeEventKind.EXECUTION_STARTED,
                graph=execution.graph,
                execution_id=execution.id,
            )
        )
        error: BaseException | None = None
        try:
            if plan is not None:
                if not isinstance(plan, ExecutionPlan):
                    raise TypeError("plan must be an ExecutionPlan")
                if plan.graph is not graph:
                    raise ValueError("execution plan belongs to a different Graph")
            result = self._executor.execute(
                execution.graph,
                graph,
                inputs,
                emit,
                plan=plan,
                slot=slot,
                options=options,
                execution=execution,
            )
            if inspect.isawaitable(result):
                result = self._async_runner().resolve(result, execution=execution)
            if result is not None:
                raise TypeError("GraphExecutor must publish outputs and return None")
            # 写入成功前先检查取消与超时，使终态事件与随后写入的状态一致。
            execution.checkpoint()
        except BaseException as exc:
            error = exc
        self._observations.publish(
            RuntimeEvent(
                RuntimeEventKind.EXECUTION_FINISHED,
                graph=execution.graph,
                execution_id=execution.id,
                status=terminal_status(error).value,
                error_type=None if error is None else type(error).__name__,
                attributes={"steps": execution.steps},
            )
        )
        try:
            if error is None:
                execution.succeed()
            else:
                execution.fail(error)
        except BaseException as exc:
            # 取消或超时恰好晚于上面的检查点时 succeed() 失败；终态事件已发布，不再补发。
            execution.fail(exc)
            raise
        if error is not None:
            raise error

    def _async_runner(self) -> LocalRunner:
        """返回等待异步执行器结果的运行器，首次调用时创建。"""

        with self._lock:
            if self._executor_runner is None:
                self._executor_runner = LocalRunner()
            return self._executor_runner

    def _emit_with_lease(self, lease: SlotLease, event: Event) -> None:
        """携带当前 Slot lease 发布跨图事件，使下游 Work 延续同一 Slot 链；未配置本地发布时使用普通发布。"""

        if self._emit_local is None:
            self._emit(event)
            return
        self._emit_local(event, lease)

    def _install_hook(self, contribution: _HookContribution) -> HookHandle:
        """把一项插件 Hook 贡献安装到执行器。"""

        return self._attach_executor_hook(
            contribution.hook,
            phase=contribution.phase,
            graph=contribution.graph,
            node=contribution.node,
        )

    def _withdraw_hook(self, contribution: _HookContribution) -> None:
        """撤销一项插件 Hook 贡献；重复调用无效果。"""

        with self._lock:
            self._hooks.withdraw(contribution)

    def _attach_executor_hook(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None,
        graph: str | None,
        node: str | None,
    ) -> HookHandle:
        """把 Hook 挂载到支持 Hook 的 GraphExecutor。

        Raises:
            TypeError: 当前 GraphExecutor 不支持 Hook。
        """

        if not isinstance(self._executor, HookableGraphExecutor):
            raise TypeError("the configured GraphExecutor does not support hooks")
        return self._executor.attach(hook, phase=phase, graph=graph, node=node)

    def _get_graph(self, name: str) -> Graph:
        """按注册名查找冻结 Graph。

        Raises:
            UnknownGraphError: 名称未注册。
        """

        name = require_non_empty_string(name, "registered graph name")
        with self._lock:
            try:
                return self._graphs[name]
            except KeyError as exc:
                raise UnknownGraphError(f"unknown registered graph {name!r}") from exc


def _noop() -> None:
    """通道已由相同配置消费时返回的空释放函数。"""


def _reject_emit(event: Event) -> None:
    """未配置事件发布能力时拒绝 Node 发出的跨图事件。

    Raises:
        InterlaceRuntimeError: 总是抛出。
    """

    del event
    raise InterlaceRuntimeError("GraphWorker has no Event emitter")
