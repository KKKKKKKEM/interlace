"""组合 Router 与 Worker 角色的 Runtime 门面；全部组件经同一 PluginHost 装配。"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from types import TracebackType
from typing import Any

from .._validation import require_non_empty_string, validate_timeout
from ..engine.core import Output
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.extension import HookBus, Registration
from ..engine.graph import ExecutionPlan, Graph
from ..engine.observation import RuntimeEvent, RuntimeEventKind
from ..engine.points import RUNTIME_EVENT
from ..plugins import (
    CAP_EVENT_ROUTER,
    CAP_GRAPH_WORKER,
    Plugin,
    PluginDescriptor,
    PluginHost,
)
from ..spi import EventHandler, GraphCatalog, RouterRole, SlotProvider, WorkerRole
from ._utils import _close_components, _Lifecycle, _remaining
from .plugin import (
    INFRASTRUCTURE,
    ROLES,
    LocalInfrastructurePlugin,
    LocalRuntimePlugin,
    required_infrastructure,
)

_LOGGER = logging.getLogger(__name__)


class Runtime:
    """组合 Router 与 Worker 角色的单进程便利门面。

    全部角色与基础设施都由插件提供：传入的插件未提供的角色由 LocalRuntimePlugin 补齐，补齐角色所需而未被
    提供的基础设施由 LocalInfrastructurePlugin 补齐。插件的钩子与具名值登记在 `hooks` 上。

    生命周期为 OPEN -> CLOSING -> CLOSED。首个 `close()` 进入 CLOSING：门面拒绝新的 emit、直接执行与
    配置变更，已接受工作及其跨图延续继续排空；随后关闭角色、停止插件并进入 CLOSED。并发的 `close()`
    等待同一次关闭完成。

    Attributes:
        router: 负责事件发布和工作路由的角色。
        worker: 负责消费工作和执行 Graph 的角色。
        _plugin_host: 管理全部插件生命周期与钩子总线的宿主。
        _lifecycle: 门面的 OPEN/CLOSING/CLOSED 关闭状态。
    """

    def __init__(
        self,
        *,
        plugins: Iterable[Plugin] | None = None,
        hooks: HookBus | None = None,
    ) -> None:
        """装配插件，补齐缺少的默认角色与基础设施，并发布 ``runtime.started`` 生命周期事实。

        Args:
            plugins: 待装配的插件；可以只提供 Router 或 Worker 中的一个。
            hooks: 预先创建的钩子总线，供在 Runtime 之外构造、需要共享总线的组件使用（例如注入的
                Engine）；None 时新建。

        Raises:
            TypeError: 插件缺少 PluginDescriptor，hooks 不是 HookBus，或提供的角色不满足
                RouterRole/WorkerRole 协议。
            ValueError: 插件声明冲突或依赖缺失。
        """

        selected = () if plugins is None else tuple(plugins)
        # 汇总 provides 之前先校验声明类型，非插件对象明确报 TypeError。
        if any(
            not isinstance(getattr(plugin, "descriptor", None), PluginDescriptor)
            for plugin in selected
        ):
            raise TypeError("plugin must define a PluginDescriptor")
        provides = {
            capability
            for plugin in selected
            for capability in plugin.descriptor.provides
        }
        defaults: list[Plugin] = []
        roles = set(ROLES) - provides
        if roles:
            defaults.append(LocalRuntimePlugin(roles=roles))
        # 补齐默认角色以及任一插件依赖或装饰、但无人提供的基础设施。
        needed = set(required_infrastructure(roles))
        for plugin in selected:
            needed.update(plugin.descriptor.requires_capabilities)
            needed.update(plugin.descriptor.wraps)
        infrastructure = (needed & set(INFRASTRUCTURE)) - provides
        if infrastructure:
            defaults.append(LocalInfrastructurePlugin(provides=infrastructure))
        host = PluginHost((*defaults, *selected), hooks=hooks)
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
        self.router: RouterRole = router
        self.worker: WorkerRole = worker
        self._plugin_host = host
        self._lifecycle = _Lifecycle("Runtime")
        self._announce(RuntimeEventKind.RUNTIME_STARTED)

    @property
    def plugin_host(self) -> PluginHost:
        """返回管理全部插件生命周期的宿主。"""

        return self._plugin_host

    @property
    def hooks(self) -> HookBus:
        """返回本 Runtime 的钩子总线，可随时登记钩子与具名值，从下一次调用生效。"""

        return self._plugin_host.hooks

    def _announce(self, kind: RuntimeEventKind) -> None:
        """经 RUNTIME_EVENT 发布 Runtime 自身的生命周期事实。"""

        self._plugin_host.hooks.notify(
            RUNTIME_EVENT, RuntimeEvent(kind), kind=kind, graph=None
        )

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

    def observe(self, event_type: str, handler: EventHandler) -> Registration:
        """注册只读事件观察者，不建立目标 Graph 路由；观察者异常不影响 emit。

        Args:
            event_type: 观察的事件类型。
            handler: 接收已接受 Event 的回调。

        Returns:
            撤销本次观察的句柄。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return _registration(self.router.observe(event_type, handler))

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
    ) -> Registration:
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
            撤销本次路由的句柄。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        limits = ExecutionLimits(max_steps, timeout, max_queued)
        return _registration(
            self.router.route(
                event_type,
                graph=graph,
                queue=queue,
                subscription=subscription,
                limits=limits,
            )
        )

    def consume(
        self,
        queue: str,
        *,
        concurrency: int = 1,
        slots: SlotProvider | int | None = None,
    ) -> Registration:
        """为命名通道注册本地消费者；concurrency 限制 Graph execution，slots 限制逻辑执行链。

        Args:
            queue: 命名消费通道。
            concurrency: 该通道同时运行的完整 Graph execution 上限，至少为 1。
            slots: 根 Work 申请 Slot 的资源池；整数创建该大小的自有池，None 时大小等于 concurrency；
                多个通道共享时传入同一个 SlotPool。

        Returns:
            撤销本次新建消费者绑定的句柄；通道此前已由相同配置消费时注销为空操作。

        Raises:
            RuntimeClosedError: Runtime 已开始关闭。
        """

        self._lifecycle.ensure_open()
        return _registration(
            self.worker.consume(queue, concurrency=concurrency, slots=slots)
        )

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

    def on(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        concurrency: int = 1,
        slots: SlotProvider | int | None = None,
        subscription: str | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        max_queued: int = 0,
    ) -> Registration:
        """原子组合 `consume()` 与 `route()`：route 失败时撤销本次新建的消费者。

        Args:
            event_type: 路由的事件类型。
            graph: 目标 Graph 的注册名。
            queue: 投递并消费 Work 的命名通道。
            concurrency: 该通道同时运行的完整 Graph execution 上限，至少为 1。
            slots: 根 Work 申请 Slot 的资源池；整数创建该大小的自有池，None 时大小等于 concurrency；
                多个通道共享时传入同一个 SlotPool。
            subscription: 竞争消费组名称，None 创建独立订阅。
            max_steps: 每个 Work execution 的 Node firing 次数上限，0 表示不限制。
            timeout: 每个 Work execution 的时限，单位秒；None 表示不限时。
            max_queued: 每个 Work execution 的 Graph 内待消费输入 token 总数上限，0 表示不限制。

        Returns:
            依次撤销路由与本次新建消费者的句柄。

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
        if type(slots) is int:
            if slots < 1:
                raise ValueError("slots must be at least 1")
        elif slots is not None and not isinstance(slots, SlotProvider):
            raise TypeError(
                "slots must be an integer, implement SlotProvider or be None"
            )
        limits = ExecutionLimits(max_steps, timeout, max_queued)

        # 先启动本地消费者，再开放对应的事件路由。
        release = self.worker.consume(queue, concurrency=concurrency, slots=slots)
        try:
            route_registration = self.router.route(
                event_type,
                graph=graph,
                queue=queue,
                subscription=subscription,
                limits=limits,
            )
        except BaseException:
            release()
            raise

        def detach() -> None:
            """先撤销路由再释放消费者，避免路由向已注销的通道投递。"""

            try:
                route_registration()
            finally:
                release()

        return Registration(detach)

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
        """进入 CLOSING 排空已接受工作，随后关闭角色、发布 ``runtime.closed`` 并执行插件清理。

        并发调用等待同一次关闭完成后返回；关闭完成后重复调用无效果。

        Raises:
            Exception: 排空或关闭时最先出现的失败；其余组件仍会关闭。
        """

        if not self._lifecycle.begin_close():
            return
        failure: BaseException | None = None
        try:
            self._announce(RuntimeEventKind.RUNTIME_CLOSING)
            try:
                # 未读取的 iter/aiter 订阅不能阻止排空；剩余输出留在存储中，订阅方之后仍可读完。
                self.worker.begin_close()
                self.wait_idle()
            except BaseException as exc:
                failure = exc
            failure = _close_components((self.worker, self.router), failure)
            self._announce(RuntimeEventKind.RUNTIME_CLOSED)
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


def _registration(handle: object) -> Registration:
    """把角色返回的注销对象统一为 Registration。

    Raises:
        TypeError: 角色返回的对象既不是 Registration 也不可调用。
    """

    if isinstance(handle, Registration):
        return handle
    if not callable(handle):
        raise TypeError("role registrations must be callable")
    return Registration(handle)
