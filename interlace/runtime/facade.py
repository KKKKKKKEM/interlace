"""组合 EventRouter 与 GraphWorker 的 Runtime 门面。"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Mapping
from types import TracebackType
from typing import Any

from .._validation import require_non_empty_string, validate_timeout
from ..engine.core import Output
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.observation import (
    CompositeObserverHandle,
    RuntimeObserver,
)
from ..engine.policies import InputSelector
from ..plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_EXECUTION_FACTORY,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_TASK_BACKEND,
    Plugin,
    PluginHost,
)
from ..spi import EventHandler, GraphCatalog, RouterRole, SlotProvider, WorkerRole
from ._utils import _close_components, _Lifecycle, _remaining
from .plugin import LocalRuntimePlugin

_LOGGER = logging.getLogger(__name__)


class Runtime:
    """组合 EventRouter 与 GraphWorker 的单进程便利门面。

    生命周期为 OPEN -> CLOSING -> CLOSED。首个 `close()` 进入 CLOSING：门面拒绝新的 emit、直接执行与
    配置变更，已接受工作及其跨图延续继续排空；随后关闭角色或插件宿主并进入 CLOSED。并发的 `close()`
    等待同一次关闭完成。

    Attributes:
        router: 负责事件发布和工作路由的角色。
        worker: 负责消费工作和执行 Graph 的角色。
        _plugin_host: 管理默认装配生命周期的宿主，显式角色注入时为 None。
        _lifecycle: 门面的 OPEN/CLOSING/CLOSED 关闭状态。
    """

    def __init__(
        self,
        *,
        router: RouterRole | None = None,
        worker: WorkerRole | None = None,
        plugins: Iterable[Plugin] | None = None,
    ) -> None:
        """通过插件宿主装配默认角色，或接管显式注入的路由与执行角色。

        Args:
            router: 事件路由角色，由 Runtime 管理生命周期。
            worker: 任务消费与 Graph 执行角色，由 Runtime 管理生命周期。
            plugins: 待装配的插件实例集合。

        Raises:
            TypeError: plugins 与显式角色混用、只注入单个角色、插件只提供部分角色，或注入对象不满足
                RouterRole/WorkerRole 协议。
        """

        if plugins is not None and (router is not None or worker is not None):
            raise TypeError("plugins cannot be combined with router or worker")
        if (router is None) != (worker is None):
            raise TypeError("Runtime requires both router and worker")
        host: PluginHost | None = None
        if router is None:
            selected = () if plugins is None else tuple(plugins)
            provides = {
                capability
                for plugin in selected
                for capability in plugin.descriptor.provides
            }
            role_capabilities = {CAP_EVENT_ROUTER, CAP_GRAPH_WORKER}
            if provides & role_capabilities and not role_capabilities <= provides:
                raise TypeError("plugins must provide both EventRouter and GraphWorker")
            if not role_capabilities <= provides:
                infrastructure = {
                    CAP_EVENT_BUS,
                    CAP_TASK_BACKEND,
                    CAP_GRAPH_EXECUTOR,
                    CAP_EXECUTION_FACTORY,
                }
                selected = (
                    LocalRuntimePlugin(infrastructure=infrastructure - provides),
                    *selected,
                )
            host = PluginHost(selected)
            try:
                host.start()
                router = host.require(CAP_EVENT_ROUTER)
                worker = host.require(CAP_GRAPH_WORKER)
                if not isinstance(router, RouterRole):
                    raise TypeError("router must implement RouterRole")
                if not isinstance(worker, WorkerRole):
                    raise TypeError("worker must implement WorkerRole")
            except BaseException:
                try:
                    host.close()
                except BaseException:
                    _LOGGER.exception("failed to roll back Runtime plugin host")
                raise
        else:
            if not isinstance(router, RouterRole):
                raise TypeError("router must implement RouterRole")
            if not isinstance(worker, WorkerRole):
                raise TypeError("worker must implement WorkerRole")
        self.router = router
        self.worker = worker
        self._plugin_host = host
        self._lifecycle = _Lifecycle("Runtime")

    @property
    def plugin_host(self) -> PluginHost | None:
        """返回插件装配宿主；显式角色组合没有宿主。

        Returns:
            管理默认装配生命周期的宿主，显式角色注入时为 None。
        """

        return self._plugin_host

    def register(self, name: str, graph: Graph) -> Runtime:
        """以稳定名称注册 Graph，未冻结时由 Worker 冻结。

        Args:
            name: 非空注册名，同一 Runtime 内唯一。
            graph: 待注册的 Graph。

        Returns:
            当前 Runtime，便于链式组合。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        self.worker.register(name, graph)
        return self

    def graphs(self) -> Mapping[str, Graph]:
        """通过公开图目录能力读取当前注册快照。

        Returns:
            名称到冻结 Graph 的只读映射。

        Raises:
            TypeError: 注入的 Worker 未提供 GraphCatalog 能力。
        """

        if not isinstance(self.worker, GraphCatalog):
            raise TypeError("worker must implement GraphCatalog for graph inspection")
        return self.worker.graphs()

    def observe(self, event_type: str, handler: EventHandler) -> Runtime:
        """注册只读事件观察者，不建立目标 Graph 路由；观察者异常不影响 emit。

        Args:
            event_type: 观察的事件类型。
            handler: 接收已接受 Event 的回调。

        Returns:
            当前 Runtime，便于链式组合。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        self.router.observe(event_type, handler)
        return self

    def route(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        subscription: str | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        max_queued: int = 0,
    ) -> Runtime:
        """把事件类型路由为目标 Graph 在命名通道上的 Work。

        Args:
            event_type: 路由的事件类型。
            graph: 目标 Graph 的注册名。
            queue: 投递 Work 的命名通道。
            subscription: 竞争消费组名称，None 创建独立订阅。
            max_steps: 每个 Work execution 的 Node firing 次数上限，0 表示不限制。
            timeout: 每个 Work execution 的时限，单位秒；None 表示不限时。
            max_queued: 每个 Work execution 的 Graph 内待消费输入 token 总数上限，0 表示不限制。

        Returns:
            当前 Runtime，便于链式组合。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        limits = ExecutionLimits(max_steps, timeout, max_queued)
        self.router.route(
            event_type,
            graph=graph,
            queue=queue,
            subscription=subscription,
            limits=limits,
        )
        return self

    def consume(
        self,
        queue: str,
        *,
        concurrency: int = 1,
        slots: SlotProvider | None = None,
    ) -> Runtime:
        """为命名通道注册本地消费者；concurrency 限制 Graph execution，slots 限制逻辑执行链。

        Args:
            queue: 命名消费通道。
            concurrency: 该通道同时运行的完整 Graph execution 上限，至少为 1。
            slots: 根 Work 申请 Slot 的资源池；None 由 Worker 创建大小等于 concurrency 的池。

        Returns:
            当前 Runtime，便于链式组合。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        self.worker.consume(queue, concurrency=concurrency, slots=slots)
        return self

    def emit(self, event_or_type: Event | str, payload: Any = None) -> Event:
        """发布根事件；成功返回表示事件传输已接受该 Event。

        Args:
            event_or_type: Event 实例，或与 payload 一起构造 Event 的类型字符串。
            payload: 以类型字符串构造 Event 时携带的领域数据。

        Returns:
            事件传输已经接受的 Event。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return self.router.emit(event_or_type, payload)

    def run(
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
    ) -> tuple[Output, ...]:
        """同步执行已注册 Graph，返回全部 terminal Output。

        返回时 Worker 已向 Runtime 观察者发布该执行的 execution.finished。

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
            按产生顺序排列的 terminal Output。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
            BaseException: Execution 失败、取消、超时或达到步数、队列上限时的原始异常。
        """

        return self.start(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            max_queued=max_queued,
            output_buffer=output_buffer,
            options=options,
        ).result()

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
        """启动已注册 Graph，并返回同步 terminal Output 流。

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
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return self.worker.iter(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            max_queued=max_queued,
            output_buffer=output_buffer,
            options=options,
        )

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
        """启动已注册 Graph，并返回异步 terminal Output 流。

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
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return self.worker.aiter(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            max_queued=max_queued,
            output_buffer=output_buffer,
            options=options,
        )

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
        """在后台启动已注册 Graph，并立即返回 Execution。

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
            可同步或异步等待、迭代和取消的 Execution。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return self.worker.start(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            max_queued=max_queued,
            output_buffer=output_buffer,
            options=options,
        )

    async def arun(
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
    ) -> tuple[Output, ...]:
        """异步执行已注册 Graph，等待全部 terminal Output。

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
            按产生顺序排列的 terminal Output。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
            BaseException: Execution 失败、取消、超时或达到步数、队列上限时的原始异常。
        """

        return await self.start(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            max_queued=max_queued,
            output_buffer=output_buffer,
            options=options,
        )

    def get_execution(self, execution_id: str) -> Execution:
        """按执行 ID 取得 Worker 保留的 Execution；队列 Work 的执行 ID 等于 Work.id。

        Args:
            execution_id: 非空执行 ID。

        Returns:
            保留的 Execution 句柄。
        """

        return self.worker.get_execution(execution_id)

    def executions(self) -> tuple[Execution, ...]:
        """返回 Worker 当前保留的 Execution 快照。

        Returns:
            按登记顺序排列的 Execution 句柄。
        """

        return self.worker.executions()

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
            graph: 目标 Graph 注册名，None 作用于全部 Graph。
            node: 目标节点 ID，None 作用于整张 Graph；指定时必须同时指定 graph。

        Returns:
            卸载本次注册的句柄。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return self.worker.attach(hook, phase=phase, graph=graph, node=node)

    def register_policy(self, name: str, selector: InputSelector) -> Runtime:
        """注册带命名空间的 selector contribution；使用它的 Graph 必须尚未冻结。

        Args:
            name: 带命名空间的策略名。
            selector: 只依据端口和 token 数量选择输入的实现。

        Returns:
            当前 Runtime，便于链式组合。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        self.worker.register_policy(name, selector)
        return self

    def observe_runtime(self, observer: RuntimeObserver) -> CompositeObserverHandle:
        """同时向 Router 与 Worker 注册只读生命周期观察者。

        Args:
            observer: 接收 Execution、Node、Event 与 Work 生命周期 RuntimeEvent 的观察者。

        Returns:
            同时卸载两处注册的句柄。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        router_handle = self.router.observe_runtime(observer)
        try:
            worker_handle = self.worker.observe_runtime(observer)
        except BaseException:
            router_handle.detach()
            raise
        return CompositeObserverHandle((router_handle, worker_handle))

    def on(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        concurrency: int = 1,
        slots: SlotProvider | None = None,
        subscription: str | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        max_queued: int = 0,
    ) -> Runtime:
        """原子组合 `consume()` 与 `route()`：route 失败时撤销本次新建的消费者。

        Args:
            event_type: 路由的事件类型。
            graph: 目标 Graph 的注册名。
            queue: 投递并消费 Work 的命名通道。
            concurrency: 该通道同时运行的完整 Graph execution 上限，至少为 1。
            slots: 根 Work 申请 Slot 的资源池；None 由 Worker 创建大小等于 concurrency 的池。
            subscription: 竞争消费组名称，None 创建独立订阅。
            max_steps: 每个 Work execution 的 Node firing 次数上限，0 表示不限制。
            timeout: 每个 Work execution 的时限，单位秒；None 表示不限时。
            max_queued: 每个 Work execution 的 Graph 内待消费输入 token 总数上限，0 表示不限制。

        Returns:
            当前 Runtime，便于链式组合。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        # 组合前完成全部本地参数校验，确定性错误无需撤销消费者。
        require_non_empty_string(event_type, "subscription event type")
        require_non_empty_string(graph, "route graph")
        require_non_empty_string(queue, "task queue")
        if subscription is not None:
            require_non_empty_string(subscription, "route subscription")
        if type(concurrency) is not int:
            raise TypeError("queue concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("queue concurrency must be at least 1")
        if slots is not None and not isinstance(slots, SlotProvider):
            raise TypeError("slots must implement SlotProvider or be None")
        limits = ExecutionLimits(max_steps, timeout, max_queued)

        # 先启动本地消费者，再开放对应的事件路由。
        release = self.worker.consume(queue, concurrency=concurrency, slots=slots)
        try:
            self.router.route(
                event_type,
                graph=graph,
                queue=queue,
                subscription=subscription,
                limits=limits,
            )
        except BaseException:
            release()
            raise
        return self

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待 Router 与 Worker 之间的级联工作全部静止。

        Args:
            timeout: 最长等待秒数，None 表示不限时。

        Raises:
            TimeoutError: 未在时限内静止。
        """

        validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self.router.wait_idle(_remaining(deadline))
            self.worker.wait_idle(_remaining(deadline))
            if self.router.idle and self.worker.idle:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Runtime did not become idle before timeout")

    def close(self) -> None:
        """进入 CLOSING 排空已接受工作，随后关闭插件宿主或显式注入的角色。

        并发调用等待同一次关闭完成后返回；关闭完成后重复调用无效果。

        Raises:
            Exception: 排空或关闭时最先出现的失败；其余组件仍会关闭。
        """

        if not self._lifecycle.begin_close():
            return
        failure: BaseException | None = None
        try:
            try:
                self.wait_idle()
            except Exception as exc:
                failure = exc
            if self._plugin_host is None:
                failure = _close_components((self.worker, self.router), failure)
            else:
                failure = _close_components((self._plugin_host,), failure)
        finally:
            self._lifecycle.finish_close()
        if failure is not None:
            raise failure

    def __enter__(self) -> Runtime:
        """进入资源作用域。

        Returns:
            当前 Runtime。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """关闭 Runtime；作用域内已有异常时不以关闭失败覆盖它。"""

        del exc_type, traceback
        try:
            self.close()
        except Exception:
            if exc_value is None:
                raise
