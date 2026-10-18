"""只负责执行单张 Graph 的内核。"""

from __future__ import annotations

import inspect
import logging
import traceback
from collections import deque
from collections.abc import (
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
)
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .._validation import require_non_empty_string
from .core import Output
from .errors import (
    ExecutionControlError,
    ExecutionError,
    HookExecutionError,
    IncompleteInputsError,
    InvalidOutputError,
    PolicySelectionError,
    QueueLimitExceededError,
    PortValueTypeError,
)
from .events import Context, Event, bind_context
from .execution import Execution, ExecutionStatus
from .graph import Edge, ExecutionPlan, Graph, NodeSpec
from .hooks import (
    HookHandle,
    HookPhase,
    HookRegistry,
    HookSignal,
    NodeCall,
    NodeHook,
    ShortCircuit,
    StopGraph,
)
from .observation import (
    ObservationHub,
    RuntimeEvent,
    RuntimeEventKind,
    node_observation_scope,
)
from .runner import LocalRunner
from .slots import Slot

Emit = Callable[[Event], None]

# 诊断回调失败只写基础设施日志，不参与业务异常恢复。
_LOGGER = logging.getLogger(__name__)

# 不交给 NodeHook.error 的异常：执行控制与 Engine 自身的契约违规。
_UNRECOVERABLE_ERRORS = (
    ExecutionControlError,
    HookExecutionError,
    InvalidOutputError,
    PortValueTypeError,
    PolicySelectionError,
)

# NODE_FINISHED 失败详情中 error 与 traceback 文本各自的最大字符数。
_ERROR_DETAIL_LIMIT = 65536


def _attribute(error: ExecutionError, graph: str, node: str) -> None:
    """为缺少归属的 ExecutionError 补充 graph 与 node，已有归属保持不变。"""

    if error.graph is None:
        error.graph = graph
    if error.node is None:
        error.node = node


@dataclass(slots=True)
class _Run:
    """一次 Graph execution 的调度状态，只在执行线程内读写。

    Attributes:
        graph_name: Graph 的注册名称，用于错误归属与观测事件。
        graph: 冻结 Graph。
        execution: 本次执行句柄。
        emit: 跨图事件发布回调。
        slot: 当前逻辑执行链的 Slot；没有时为 None。
        options: 本次执行配置，每次 firing 由 bind_context 深复制。
        entrypoint: 本次执行的入口节点 ID。
        outgoing_for: 原图或 ExecutionPlan 的下游连接查询。
        specs: 计划内节点的冻结元数据。
        input_names: 计划内节点按声明顺序排列的输入端口名称。
        hooks: 计划内各节点在 Hook 快照中生效的 Hook，按注册顺序排列。
        queues: 计划内各节点各输入端口的待消费 token。
        queued: queues 中全部待消费 token 的总数，用于执行 max_queued 限制。
        ready: 待触发的 (节点 ID, 选中端口)；选中端口在入队时计算，ON_START 为空元组。
        scheduled: 已在 ready 中的节点，同一节点最多排队一次。
        started: 已触发过的 ON_START 节点。
        local_state: Context.state 的 execution 局部存储。
        finalizers: Context.on_quiescence 注册的回调，按注册顺序排列。
        inspect_failures: inspect 失败的 Hook（按 id）到 (类型名称, 失败次数) 的映射。
    """

    graph_name: str
    graph: Graph
    execution: Execution
    emit: Emit
    slot: Slot | None
    options: Mapping[str, Any] | None
    entrypoint: str
    outgoing_for: Callable[[str, str], tuple[Edge, ...]]
    specs: dict[str, NodeSpec]
    input_names: dict[str, tuple[str, ...]]
    hooks: dict[str, tuple[NodeHook, ...]]
    queues: dict[str, dict[str, deque[Any]]]
    ready: deque[tuple[str, tuple[str, ...]]] = field(default_factory=deque)
    scheduled: set[str] = field(default_factory=set)
    started: set[str] = field(default_factory=set)
    local_state: dict[tuple[str, str], MutableMapping[str, Any]] = field(
        default_factory=dict
    )
    finalizers: list[Callable[[], None]] = field(default_factory=list)
    inspect_failures: dict[int, tuple[str, int]] = field(default_factory=dict)
    queued: int = 0

    def is_cancelled(self) -> bool:
        """返回 execution 是否已接受取消请求。"""

        return self.execution.cancel_requested

    def schedule_if_ready(self, node_id: str) -> None:
        """节点未排队且输入策略选中端口时，把节点与本次选择加入 ready。

        缓存的选择在出队时仍然有效：只有该节点自身消费它的队列，排队期间只会追加 token。
        """

        if node_id in self.scheduled:
            return
        if self.specs[node_id].input_policy.on_start:
            first = node_id == self.entrypoint and node_id not in self.started
            selection: tuple[str, ...] | None = () if first else None
        else:
            selection = self._select(node_id)
        if selection is not None:
            self.ready.append((node_id, selection))
            self.scheduled.add(node_id)

    def enqueue(self, node_id: str, port: str, value: Any, source: str | None) -> None:
        """把一个 token 放入节点输入队列，超过 max_queued 时终止执行。

        Args:
            node_id: 接收 token 的节点 ID。
            port: 接收 token 的输入端口。
            value: 待消费的值。
            source: 产生该值的节点 ID；入口输入为 None。

        Raises:
            QueueLimitExceededError: 入队后待消费 token 总数会超过 max_queued。
        """

        limit = self.execution.limits.max_queued
        if limit and self.queued >= limit:
            largest = sorted(
                (
                    (len(values), f"{target}.{name}")
                    for target, ports in self.queues.items()
                    for name, values in ports.items()
                    if values
                ),
                reverse=True,
            )[:3]
            backlog = ", ".join(f"{name}={count}" for count, name in largest)
            raise QueueLimitExceededError(
                f"execution exceeded max_queued={limit} pending inputs "
                f"while queuing {node_id}.{port}; largest queues: {backlog}",
                graph=self.graph_name,
                node=source,
            )
        self.queues[node_id][port].append(value)
        self.queued += 1

    def take(self, node_id: str, selection: tuple[str, ...]) -> dict[str, Any]:
        """从选中端口各取出一个 token；ON_START 节点同时记为已触发。"""

        if self.specs[node_id].input_policy.on_start:
            self.started.add(node_id)
        queues = self.queues[node_id]
        self.queued -= len(selection)
        return {port: queues[port].popleft() for port in selection}

    def _select(self, node_id: str) -> tuple[str, ...] | None:
        """调用节点的输入策略，并为选择失败补齐 Graph 与节点归属。

        Raises:
            PolicySelectionError: selector 违反契约或抛出异常；后者以原异常为 __cause__。
        """

        try:
            return self.specs[node_id].input_policy.select(
                self.input_names[node_id], self.queues[node_id]
            )
        except PolicySelectionError as exc:
            if exc.graph is None:
                exc.graph = self.graph_name
            if exc.node is None:
                exc.node = node_id
            raise
        except Exception as exc:
            raise PolicySelectionError(
                f"input policy of node {node_id!r} failed: {exc}",
                graph=self.graph_name,
                node=node_id,
            ) from exc


@dataclass(frozen=True, slots=True)
class _Firing:
    """一次 Node firing 在 Hook 管道内共享的参数。

    Attributes:
        run: 所属 execution 的调度状态。
        node_id: 被触发的节点 ID。
        original: Context 工厂之后、全部 Hook enter 之前的调用，用于校验调用身份。
        hooks: 对该节点生效的 Hook，按注册顺序排列。
    """

    run: _Run
    node_id: str
    original: NodeCall
    hooks: tuple[NodeHook, ...]


class Engine:
    """默认 GraphExecutor：在调用线程内执行 Graph 的 Node、InputPolicy、Output 和 Edge。

    一次 execution 内的 Node firing 严格串行；异步 Node 与 Hook 由私有的 LocalRunner
    loop 线程等待。

    Attributes:
        hooks: 动态 Hook 注册表；每次 execution 开始时取快照。
        _runner: 等待 awaitable 的后台 Runner，由 Engine 关闭。
        _observations: 发布 Node 与输出观测事件的分发中心。
    """

    def __init__(
        self,
        *,
        hooks: HookRegistry | None = None,
        runner: LocalRunner | None = None,
        observations: ObservationHub | None = None,
    ) -> None:
        """组装 Hook 注册表、异步 Runner 和观测分发中心。

        Args:
            hooks: Hook 注册表；None 时新建。
            runner: 等待 awaitable 的 Runner；None 时新建，close 时一并关闭。
            observations: 观测分发中心；None 时新建独立实例。
        """

        self.hooks = HookRegistry() if hooks is None else hooks
        self._runner = LocalRunner() if runner is None else runner
        self._observations = ObservationHub() if observations is None else observations

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
    ) -> None:
        """在调用线程内执行一张冻结 Graph，并通过 execution 发布 terminal Output。

        不调用 execution 的 succeed/fail；成功返回表示数据流已自然静止并完成静止回调，
        或 Hook 以 StopGraph 停止了 Graph。

        Args:
            name: Graph 的注册名称，必须与 execution.graph 相同。
            graph: 冻结 Graph。
            inputs: 入口输入；单端口入口时整体作为该端口的值，多端口时为端口映射。
            emit: 接收 Context.emit 产生的 Event 的回调。
            plan: 属于 graph 的 ExecutionPlan；None 执行完整 Graph。
            slot: 当前逻辑执行链的 Slot；没有时为 None。
            options: 本次执行配置，每次 firing 深复制后交给 Context。
            execution: 已处于 RUNNING 的执行句柄。

        Raises:
            TypeError: 参数类型不符。
            ValueError: execution 或 plan 属于其他 Graph。
            RuntimeError: Graph 未冻结或 execution 不处于 RUNNING。
            PortValueTypeError: 入口输入不符合入口端口声明。
            ExecutionError: 节点、Hook、输入策略或静止回调失败，以及执行控制异常。
        """

        name = require_non_empty_string(name, "graph name")
        if not isinstance(graph, Graph):
            raise TypeError("graph must be a Graph")
        if not callable(emit):
            raise TypeError("emit must be callable")
        if slot is not None and not isinstance(slot, Slot):
            raise TypeError("slot must be a Slot or None")
        if not isinstance(execution, Execution):
            raise TypeError("execution must be an Execution")
        elif execution.graph != name:
            raise ValueError("execution belongs to a different registered Graph")
        if not graph.frozen:
            raise RuntimeError("Engine requires a frozen Graph")
        if plan is not None:
            if not isinstance(plan, ExecutionPlan):
                raise TypeError("plan must be an ExecutionPlan")
            if plan.graph is not graph:
                raise ValueError("execution plan belongs to a different Graph")
        if execution.status is not ExecutionStatus.RUNNING:
            raise RuntimeError("execution must be running")
        entrypoint = graph.entrypoint if plan is None else plan.entrypoint
        try:
            prepared = self._coerce_inputs(graph, inputs, entrypoint=entrypoint)
        except ExecutionError as exc:
            _attribute(exc, name, entrypoint)
            raise
        run = self._prepare(
            name,
            graph,
            prepared,
            emit,
            self.hooks.snapshot(name),
            plan,
            slot,
            execution,
            options,
        )
        self._run(run)

    def close(self) -> None:
        """关闭 Hook 注册表和后台异步 Runner。"""

        self.hooks.close()
        self._runner.close()

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """注册动态 Hook，实现 HookableGraphExecutor 能力；参数见 `HookRegistry.attach`。

        Returns:
            卸载本次注册的句柄。
        """

        return self.hooks.attach(hook, phase=phase, graph=graph, node=node)

    def _prepare(
        self,
        graph_name: str,
        graph: Graph,
        initial_inputs: Mapping[str, Any],
        emit: Emit,
        hook_snapshot: tuple[Any, ...],
        plan: ExecutionPlan | None,
        slot: Slot | None,
        execution: Execution,
        options: Mapping[str, Any] | None,
    ) -> _Run:
        """建立计划内节点的队列与 Hook，注入入口输入并调度入口节点。

        Raises:
            PolicySelectionError: 入口节点的输入策略失败。
            QueueLimitExceededError: 入口输入数量超过 max_queued。
        """

        active = set(graph.nodes) if plan is None else plan.nodes
        specs = {
            node_id: graph.spec_for(node_id)
            for node_id in graph.nodes
            if node_id in active
        }
        run = _Run(
            graph_name=graph_name,
            graph=graph,
            execution=execution,
            emit=emit,
            slot=slot,
            options=options,
            entrypoint=graph.entrypoint if plan is None else plan.entrypoint,
            outgoing_for=graph.outgoing_for if plan is None else plan.outgoing_for,
            specs=specs,
            input_names={
                node_id: tuple(spec.input_ports) for node_id, spec in specs.items()
            },
            hooks={
                node_id: self.hooks.for_node(hook_snapshot, node_id)
                for node_id in specs
            },
            queues={
                node_id: {port: deque() for port in spec.input_ports}
                for node_id, spec in specs.items()
            },
        )
        for port, value in initial_inputs.items():
            run.enqueue(run.entrypoint, port, value, None)
        run.schedule_if_ready(run.entrypoint)
        return run

    def _run(self, run: _Run) -> None:
        """按就绪顺序触发节点，直至数据流静止、StopGraph 或失败。

        自然静止后先拒绝残留输入，再运行静止回调。

        Raises:
            IncompleteInputsError: 静止时仍有无法组成输入的 token。
            ExecutionError: 节点、Hook、输入策略或静止回调失败，以及执行控制异常。
        """

        try:
            while run.ready:
                run.execution.checkpoint()
                node_id, selection = run.ready.popleft()
                run.scheduled.remove(node_id)
                if not self._fire(run, node_id, selection):
                    return
                # 每轮只消费一组输入，避免活跃循环使其他节点长期无法执行。
                run.schedule_if_ready(node_id)
            run.execution.checkpoint()
            self._check_leftovers(run)
            self._run_quiescence(run)
        finally:
            self._report_inspect_failures(run)

    def _fire(self, run: _Run, node_id: str, selection: tuple[str, ...]) -> bool:
        """执行一次 Node firing 并发布开始与结束观测事件。

        Returns:
            Hook 以 StopGraph 停止 Graph 时返回 False，否则返回 True。
        """

        execution = run.execution
        inputs = MappingProxyType(run.take(node_id, selection))
        started_event = RuntimeEvent(
            RuntimeEventKind.NODE_STARTED,
            graph=run.graph_name,
            execution_id=execution.id,
            node=node_id,
            attributes={"step": execution.steps + 1},
        )
        started = False
        error: BaseException | None = None
        try:
            with execution.step(node_id), node_observation_scope(started_event):
                started = True
                self._observations.publish(started_event)
                context = self._make_context(run, node_id, inputs)
                execution.checkpoint()
                self._drain_node(run, node_id, inputs, context)
        except StopGraph as signal:
            try:
                self._publish_stopped(run, node_id, signal)
            except BaseException as exc:
                error = exc
                raise
            return False
        except BaseException as exc:
            error = exc
            raise
        finally:
            if started:
                self._observations.publish(self._node_finished(run, node_id, error))
        return True

    def _make_context(
        self, run: _Run, node_id: str, inputs: Mapping[str, Any]
    ) -> Context:
        """绑定本次 firing 的基础 Context，并交给 Graph 的 context_factory。

        Raises:
            ExecutionControlError: 工厂内检测到取消或超时，原样传播。
            ExecutionError: 绑定或工厂失败、工厂未返回 Context；原异常为 __cause__。
            HookExecutionError: 工厂抛出只允许 Hook 使用的 StopGraph 或 ShortCircuit。
        """

        try:
            base = bind_context(
                run.emit,
                run.slot,
                checkpoint=run.execution.checkpoint,
                is_cancelled=run.is_cancelled,
                local=run.local_state,
                finalizers=run.finalizers,
                scope=node_id,
                options=run.options,
            )
            context = run.graph.context_factory(base, inputs)
            if not isinstance(context, Context):
                if inspect.iscoroutine(context):
                    context.close()
                raise TypeError("context_factory must return a Context")
        except ExecutionControlError:
            raise
        except HookSignal as signal:
            raise HookExecutionError(
                f"{type(signal).__name__} can only be raised by a hook",
                graph=run.graph_name,
                node=node_id,
            ) from signal
        except Exception as exc:
            raise ExecutionError(
                f"context factory failed: {exc}",
                graph=run.graph_name,
                node=node_id,
            ) from exc
        return context

    def _drain_node(
        self,
        run: _Run,
        node_id: str,
        inputs: Mapping[str, Any],
        context: Context,
    ) -> None:
        """逐项读取 Hook 管道输出并路由，结束或失败时关闭管道。

        Raises:
            HookExecutionError: Hook 在 enter 以外的阶段抛出 ShortCircuit。
        """

        try:
            with closing(self._call_node(run, node_id, inputs, context)) as outputs:
                for output in outputs:
                    self._route_output(run, node_id, output)
        except ShortCircuit as exc:
            raise HookExecutionError(
                "ShortCircuit is only valid during hook enter",
                graph=run.graph_name,
                node=node_id,
            ) from exc

    def _route_output(self, run: _Run, node_id: str, output: Output) -> None:
        """校验一项输出，沿下游 Edge 入队并调度目标；没有下游时发布为 terminal Output。

        Raises:
            InvalidOutputError: 端口未声明。
            PortValueTypeError: 值不符合输出端口或下游输入端口类型。
            PolicySelectionError: 下游节点的输入策略失败。
            QueueLimitExceededError: 入队后待消费 token 总数会超过 max_queued。
        """

        self._check_output(run, node_id, output)
        edges = run.outgoing_for(node_id, output.port)
        if not edges:
            run.execution.publish_output(output)
        for edge in edges:
            target_type = run.specs[edge.target].input_ports[edge.target_port]
            if not isinstance(output.value, target_type):
                raise PortValueTypeError(
                    f"edge target {edge.target}.{edge.target_port} "
                    f"expected {target_type.__name__}",
                    graph=run.graph_name,
                    node=node_id,
                )
            run.enqueue(edge.target, edge.target_port, output.value, node_id)
            run.schedule_if_ready(edge.target)
        self._observations.publish(
            RuntimeEvent(
                RuntimeEventKind.OUTPUT_ROUTED,
                graph=run.graph_name,
                execution_id=run.execution.id,
                node=node_id,
                attributes={
                    "step": run.execution.steps,
                    "port": output.port,
                    "targets": tuple((edge.target, edge.target_port) for edge in edges),
                },
            )
        )

    @staticmethod
    def _check_output(run: _Run, node_id: str, output: Output) -> None:
        """校验输出端口已由节点声明且值符合端口类型。

        Raises:
            InvalidOutputError: 端口未声明。
            PortValueTypeError: 值不符合端口类型。
        """

        declared = run.specs[node_id].output_ports
        if output.port not in declared:
            raise InvalidOutputError(
                f"node {node_id!r} produced unknown port {output.port!r}",
                graph=run.graph_name,
                node=node_id,
            )
        expected = declared[output.port]
        if not isinstance(output.value, expected):
            raise PortValueTypeError(
                f"node {node_id!r} output {output.port!r} expected "
                f"{expected.__name__}, got {type(output.value).__name__}",
                graph=run.graph_name,
                node=node_id,
            )

    def _publish_stopped(self, run: _Run, node_id: str, signal: StopGraph) -> None:
        """按停止节点的输出端口契约校验 StopGraph 携带的输出，并发布为 terminal Output。

        Raises:
            InvalidOutputError: 输出不是 Output 或端口未声明。
            PortValueTypeError: 值不符合端口类型。
        """

        with self._coerce_outputs(
            signal.outputs, run.graph_name, node_id, "StopGraph", run.execution
        ) as stopped:
            for output in stopped:
                self._check_output(run, node_id, output)
                run.execution.publish_output(output)

    @staticmethod
    def _node_finished(
        run: _Run, node_id: str, error: BaseException | None
    ) -> RuntimeEvent:
        """构造 NODE_FINISHED 事件；失败时附带截断的异常文本与 traceback。"""

        details: dict[str, Any] = {}
        if error is not None:
            try:
                details = {
                    "error": str(error)[:_ERROR_DETAIL_LIMIT],
                    "traceback": "".join(
                        traceback.format_exception(
                            type(error), error, error.__traceback__
                        )
                    )[:_ERROR_DETAIL_LIMIT],
                }
            except Exception:
                details = {"error": type(error).__name__}
        return RuntimeEvent(
            RuntimeEventKind.NODE_FINISHED,
            graph=run.graph_name,
            execution_id=run.execution.id,
            node=node_id,
            status="failed" if error is not None else "succeeded",
            error_type=None if error is None else type(error).__name__,
            attributes={"step": run.execution.steps, **details},
        )

    @staticmethod
    def _check_leftovers(run: _Run) -> None:
        """数据流静止时拒绝仍留在输入队列中的 token。

        Raises:
            IncompleteInputsError: 存在残留 token，消息列出各端口的残留数量。
        """

        leftovers = {
            f"{node_id}.{port}": len(values)
            for node_id, ports in run.queues.items()
            for port, values in ports.items()
            if values
        }
        if leftovers:
            raise IncompleteInputsError(
                f"graph stopped with incomplete inputs: {leftovers!r}",
                graph=run.graph_name,
            )

    @staticmethod
    def _run_quiescence(run: _Run) -> None:
        """按注册顺序运行静止回调，包括回调期间新注册的回调。

        每个回调前检查取消与超时；失败语义见 `Context.on_quiescence`。

        Raises:
            ExecutionControlError: 回调前或回调内检测到取消或超时。
            ExecutionError: 回调失败；普通异常被包装并以原异常为 __cause__。
            HookExecutionError: 回调抛出只允许 Hook 使用的 StopGraph 或 ShortCircuit。
        """

        index = 0
        while index < len(run.finalizers):
            run.execution.checkpoint()
            callback = run.finalizers[index]
            index += 1
            try:
                callback()
            except ExecutionControlError:
                raise
            except HookSignal as signal:
                raise HookExecutionError(
                    f"{type(signal).__name__} can only be raised by a hook",
                    graph=run.graph_name,
                ) from signal
            except ExecutionError as exc:
                if exc.graph is None:
                    exc.graph = run.graph_name
                raise
            except Exception as exc:
                raise ExecutionError(
                    f"quiescence callback failed: {exc}",
                    graph=run.graph_name,
                ) from exc

    def _call_node(
        self,
        run: _Run,
        node_id: str,
        inputs: Mapping[str, Any],
        context: Context,
    ) -> Generator[Output, None, None]:
        """惰性产出经过全部 Hook 转换的节点输出，并统一失败归属。

        Yields:
            经过 Hook exit 转换的 Output，尚未按端口契约校验。

        Raises:
            ExecutionError: 节点或 Hook 抛出普通异常时包装为带节点归属的错误；
                ExecutionError 子类原样传播，缺少归属时补充当前 graph 与 node。
            HookSignal: Hook 抛出的 ShortCircuit 或 StopGraph，由调用方解释。
        """

        firing = _Firing(
            run,
            node_id,
            NodeCall(
                run.graph_name, node_id, run.graph.nodes[node_id], inputs, context
            ),
            run.hooks[node_id],
        )
        try:
            yield from self._invoke(firing, 0, firing.original)
        except HookSignal:
            raise
        except ExecutionError as exc:
            _attribute(exc, run.graph_name, node_id)
            raise
        except Exception as exc:
            raise ExecutionError(
                f"node {node_id!r} failed: {exc}",
                graph=run.graph_name,
                node=node_id,
            ) from exc

    def _invoke(
        self, firing: _Firing, index: int, call: NodeCall
    ) -> Generator[Output, None, None]:
        """运行第 index 个 Hook 包裹的内层调用；越过最后一个 Hook 时调用 Node。

        enter 抛出 ShortCircuit 时用信号输出代替内层调用；每项内层输出都经过本 Hook exit。

        Raises:
            HookExecutionError: enter 返回值不是 NodeCall 或改变了调用身份与输入端口。
            PortValueTypeError: enter 替换的输入值不符合端口类型。
        """

        if index == len(firing.hooks):
            yield from self._invoke_node(firing, call)
            return
        execution = firing.run.execution
        hook = firing.hooks[index]
        try:
            entered = self._resolve(hook.enter(call), execution)
        except ShortCircuit as signal:
            entered = call
            inner = self._produce(firing, signal.outputs, "ShortCircuit")
        else:
            entered = self._validate_hook_call(
                firing.original,
                entered,
                firing.run.specs[firing.node_id].input_ports,
                firing.run.graph_name,
                firing.node_id,
            )
            inner = self._recover(
                firing, self._invoke(firing, index + 1, entered), hook, entered
            )
        with closing(inner):
            for output in inner:
                yield from self._produce(
                    firing, self._resolve(hook.exit(entered, output), execution), "Hook"
                )

    def _invoke_node(
        self, firing: _Firing, call: NodeCall
    ) -> Generator[Output, None, None]:
        """调用 Node 并逐项产出原始输出，在实际调用边界通知 inspect。

        Raises:
            HookExecutionError: Node 自身抛出 ShortCircuit 或 StopGraph。
        """

        try:
            self._inspect(firing, call, HookPhase.ENTER, None)
            result = self._resolve(
                call.node.execute(call.inputs, call.context), firing.run.execution
            )
            with closing(self._produce(firing, result, "Node")) as outputs:
                for output in outputs:
                    self._inspect(firing, call, HookPhase.EXIT, output)
                    yield output
        except HookSignal as signal:
            raise HookExecutionError(
                f"{type(signal).__name__} can only be raised by a hook",
                graph=firing.run.graph_name,
                node=firing.node_id,
            ) from signal
        except GeneratorExit:
            raise
        except BaseException as exc:
            self._inspect(firing, call, HookPhase.ERROR, exc)
            raise

    def _recover(
        self,
        firing: _Firing,
        outputs: Generator[Output, None, None],
        hook: NodeHook,
        call: NodeCall,
    ) -> Generator[Output, None, None]:
        """把内层输出流的业务异常交给 hook.error；已交付的输出前缀不重放。

        执行控制异常与 Engine 契约错误（见 `_UNRECOVERABLE_ERRORS`）原样传播。
        """

        with closing(outputs):
            try:
                yield from outputs
            except _UNRECOVERABLE_ERRORS:
                raise
            except Exception as error:
                yield from self._produce(
                    firing,
                    self._resolve(hook.error(call, error), firing.run.execution),
                    "Hook",
                )

    def _produce(
        self, firing: _Firing, value: object, source: str
    ) -> Generator[Output, None, None]:
        """逐项产出 value 规范化后的 Output，结束或中断时关闭其源迭代器。"""

        with self._coerce_outputs(
            value, firing.run.graph_name, firing.node_id, source, firing.run.execution
        ) as outputs:
            yield from outputs

    def _inspect(
        self, firing: _Firing, call: NodeCall, phase: HookPhase, value: Any
    ) -> None:
        """同步通知全部 Hook 的 inspect；失败被隔离，不改变调用或输出。"""

        for hook in firing.hooks:
            try:
                callback: Callable[[NodeCall, HookPhase, Any], object] = hook.inspect
                result = callback(call, phase, value)
                if result is not None:
                    if inspect.iscoroutine(result):
                        result.close()
                    raise TypeError("NodeHook.inspect must return None synchronously")
            # HookSignal 继承 BaseException，inspect 发出的控制信号同样属于采集失败。
            except (Exception, HookSignal):
                self._record_inspect_failure(firing.run, hook)

    @staticmethod
    def _record_inspect_failure(run: _Run, hook: NodeHook) -> None:
        """计数 inspect 失败；每个 Hook 在一次 execution 内只记录首次失败的 traceback。

        必须在 except 块内调用，以便日志携带当前异常。
        """

        key = id(hook)
        name, count = run.inspect_failures.get(key, (type(hook).__qualname__, 0))
        run.inspect_failures[key] = (name, count + 1)
        if count == 0:
            _LOGGER.exception(
                "NodeHook.inspect of %s failed in execution %s; "
                "later failures in this execution are only counted",
                name,
                run.execution.id,
            )

    @staticmethod
    def _report_inspect_failures(run: _Run) -> None:
        """execution 结束时汇总失败多于一次的 inspect Hook。"""

        for name, count in run.inspect_failures.values():
            if count > 1:
                _LOGGER.warning(
                    "NodeHook.inspect of %s failed %d times in execution %s",
                    name,
                    count,
                    run.execution.id,
                )

    def _resolve(self, value: object, execution: Execution) -> object:
        """原样返回同步值，或在 execution 的取消与超时约束下等待 awaitable。"""

        return self._runner.resolve(value, execution=execution)

    @staticmethod
    @contextmanager
    def _coerce_outputs(
        result: object,
        graph_name: str,
        node_id: str | None,
        source: str,
        execution: Execution,
    ) -> Iterator[Iterator[Output]]:
        """接管输出源并提供逐项校验的迭代器，退出时关闭源而无需耗尽它。

        Args:
            result: Node 或 Hook 的返回值：Output、Iterable[Output] 或 None。
            graph_name: 错误归属的 Graph 注册名称。
            node_id: 错误归属的节点 ID。
            source: 错误消息中的来源名称，如 "Node"、"Hook"、"StopGraph"。
            execution: 每次推进源前后检查控制条件的执行句柄。

        Yields:
            每次只推进源一项、并拒绝非 Output 元素的迭代器。

        Raises:
            InvalidOutputError: result 不是允许的返回形态，或元素不是 Output。
        """

        try:
            iterator = Engine._iter_outputs(result)
        except TypeError as exc:
            raise InvalidOutputError(
                f"{source} returned invalid output: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc

        def checked() -> Generator[Output, None, None]:
            """在每次推进前后检查控制条件，并拒绝非 Output 元素。"""

            while True:
                execution.checkpoint()
                try:
                    output = next(iterator)
                except StopIteration:
                    return
                execution.checkpoint()
                if not isinstance(output, Output):
                    raise InvalidOutputError(
                        f"{source} output iterable must contain only Output",
                        graph=graph_name,
                        node=node_id,
                    )
                yield output

        stream = checked()
        try:
            yield stream
        finally:
            try:
                stream.close()
            finally:
                close = getattr(iterator, "close", None)
                if close is not None:
                    close()

    @staticmethod
    def _validate_hook_call(
        original: NodeCall,
        modified: object,
        input_ports: Mapping[str, type[Any]],
        graph_name: str,
        node_id: str,
    ) -> NodeCall:
        """确认 Hook enter 只替换了输入值，且新值符合输入端口类型。

        Returns:
            通过校验的 modified。

        Raises:
            HookExecutionError: modified 不是 NodeCall，改变了 Graph、节点或 Context，
                或增删了输入端口。
            PortValueTypeError: 输入值不符合端口类型。
        """

        if not isinstance(modified, NodeCall):
            raise HookExecutionError(
                "hook enter must return NodeCall",
                graph=graph_name,
                node=node_id,
            )
        if (
            modified.graph != original.graph
            or modified.node_id != original.node_id
            or modified.node is not original.node
            or modified.context is not original.context
        ):
            raise HookExecutionError(
                "hook cannot replace graph, node, or context",
                graph=graph_name,
                node=node_id,
            )
        if set(modified.inputs) != set(original.inputs):
            raise HookExecutionError(
                "hook cannot add or remove node input ports",
                graph=graph_name,
                node=node_id,
            )
        for port, value in modified.inputs.items():
            expected = input_ports[port]
            if not isinstance(value, expected):
                raise PortValueTypeError(
                    f"hook input {port!r} expected {expected.__name__}, "
                    f"got {type(value).__name__}",
                    graph=graph_name,
                    node=node_id,
                )
        return modified

    @staticmethod
    def _iter_outputs(result: object) -> Iterator[object]:
        """取得返回值的原始迭代器；元素由 `_coerce_outputs` 逐项校验。

        Raises:
            TypeError: result 不是 Output、None 或非 str/bytes/Mapping 的 Iterable。
        """

        if result is None:
            return iter(())
        if isinstance(result, Output):
            return iter((result,))
        if not isinstance(result, Iterable) or isinstance(
            result, (str, bytes, Mapping)
        ):
            raise TypeError("Node must return Output, Iterable[Output], or None")

        return iter(result)

    @staticmethod
    def _coerce_inputs(
        graph: Graph, value: Any, *, entrypoint: str
    ) -> Mapping[str, Any]:
        """把入口输入规范化为只读端口映射并校验类型。

        单端口入口把 value 整体作为该端口的值；多端口入口要求键集合完全匹配的 Mapping；
        零端口入口只接受 None 或空映射。

        Raises:
            PortValueTypeError: 输入形态或值类型不符合入口端口声明。
        """

        ports = graph.spec_for(entrypoint).input_ports
        names = tuple(ports)
        if not names:
            if value not in (None, {}):
                raise PortValueTypeError("graph entrypoint accepts no input")
            return MappingProxyType({})
        if len(names) == 1:
            prepared = {names[0]: value}
        elif isinstance(value, Mapping) and set(value) == set(names):
            prepared = dict(value)
        else:
            raise PortValueTypeError(f"graph entrypoint requires input ports {names!r}")
        for port, item in prepared.items():
            expected = ports[port]
            if not isinstance(item, expected):
                raise PortValueTypeError(
                    f"entry input {port!r} expected {expected.__name__}, "
                    f"got {type(item).__name__}"
                )
        return MappingProxyType(prepared)
