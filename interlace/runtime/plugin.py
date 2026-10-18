"""默认单进程 Runtime 的插件装配。"""

from __future__ import annotations

from collections.abc import Iterable
from typing import cast

from ..adapters import memory
from ..engine.execution import Execution
from ..engine.executor import Engine
from ..engine.observation import ObservationHub
from ..engine.policies import PolicyRegistry
from ..plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_EXECUTION_FACTORY,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_TASK_BACKEND,
    PluginContext,
    PluginDescriptor,
)
from ..spi import EventBus, ExecutionFactory, GraphExecutor, TaskBackend
from ._utils import _close_components, _unique
from .router import EventRouter
from .worker import GraphWorker

# 默认插件可以自行提供、也可以由其他插件提供的底层能力，按声明顺序排列。
_INFRASTRUCTURE = (
    CAP_EVENT_BUS,
    CAP_TASK_BACKEND,
    CAP_GRAPH_EXECUTOR,
    CAP_EXECUTION_FACTORY,
)


class LocalRuntimePlugin:
    """使用标准能力协议组装单进程 Runtime 的内建插件。

    插件总是提供 EventRouter 与 GraphWorker 角色；底层能力由 `infrastructure` 选择自行提供，
    其余底层能力声明为依赖，从宿主中由其他插件提供。

    Attributes:
        descriptor: 按本实例能力选择生成的插件身份、依赖和能力声明。
        _events: 自行提供时注入或在 setup 中创建的事件传输。
        _tasks: 自行提供时注入或在 setup 中创建的任务后端。
        _router_observations: 路由角色的生命周期观察中心。
        _worker_observations: 执行角色与默认 Engine 共享的生命周期观察中心。
        _policies: Worker 冻结 Graph 时使用的输入策略注册表。
        _executor: 自行提供时注入或在 setup 中创建的 Graph 执行器。
        _execution_factory: 自行提供时注入或在 setup 中选用的 Execution 工厂。
        _provided: 本插件自行提供的底层能力集合。
        _owned: 依次表示事件传输、任务后端和执行器是否由本插件创建并负责关闭。
        _close_injected: 停止时是否同时关闭调用方注入的底层组件。
        _history_limit: 传给 GraphWorker 的 Execution 记录保留上限。
        _direct_workers: 传给 GraphWorker 的直接执行线程数上限，None 使用标准库默认值。
        router: setup 后创建的事件路由角色，之前为 None。
        worker: setup 后创建的任务消费与执行角色，之前为 None。
    """

    def __init__(
        self,
        *,
        events: EventBus | None = None,
        tasks: TaskBackend | None = None,
        executor: GraphExecutor | None = None,
        execution_factory: ExecutionFactory | None = None,
        close_injected: bool = False,
        infrastructure: Iterable[str] | None = None,
        history_limit: int = 1000,
        direct_workers: int | None = None,
    ) -> None:
        """选择自行提供的底层能力，并保存注入组件与资源所有权设置。

        Args:
            events: 注入的事件传输；None 且自行提供时在 setup 中创建内存实现。
            tasks: 注入的任务后端，须同时实现 TaskPublisher 与 TaskConsumer；None 且自行提供时创建内存实现。
            executor: 注入的 Graph 执行器；None 且自行提供时创建默认 Engine。
            execution_factory: 注入的 Execution 工厂；None 且自行提供时使用 Execution。
            close_injected: 停止时是否同时关闭注入的 events、tasks 与 executor。
            infrastructure: 本插件自行提供的底层能力，取自 event bus、task backend、graph executor 与
                execution factory 四项；None 表示全部自行提供，其余能力声明为依赖。
            history_limit: 传给 GraphWorker 的 Execution 记录保留上限。
            direct_workers: 传给 GraphWorker 的直接执行线程数上限，None 使用标准库默认值。

        Raises:
            TypeError: 参数类型不符合契约，或为外部提供的能力注入了组件。
            ValueError: infrastructure 包含底层能力以外的名称。
        """

        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        if tasks is not None and not (
            callable(getattr(tasks, "submit", None))
            and callable(getattr(tasks, "bind", None))
        ):
            raise TypeError("tasks must implement TaskPublisher and TaskConsumer")
        if infrastructure is None:
            provided = frozenset(_INFRASTRUCTURE)
        else:
            if isinstance(infrastructure, str):
                raise TypeError("infrastructure must be an iterable of capabilities")
            provided = frozenset(infrastructure)
            if not provided <= frozenset(_INFRASTRUCTURE):
                raise ValueError(
                    "local runtime can only select infrastructure capabilities"
                )
        if events is not None and CAP_EVENT_BUS not in provided:
            raise TypeError(
                "events cannot be injected when event bus is externally provided"
            )
        if tasks is not None and CAP_TASK_BACKEND not in provided:
            raise TypeError(
                "tasks cannot be injected when task backend is externally provided"
            )
        if executor is not None and CAP_GRAPH_EXECUTOR not in provided:
            raise TypeError(
                "executor cannot be injected when graph executor is externally provided"
            )
        if execution_factory is not None and CAP_EXECUTION_FACTORY not in provided:
            raise TypeError("execution_factory is externally provided")
        self.descriptor = PluginDescriptor(
            "interlace.core/local-runtime",
            "1.0.0",
            requires_capabilities=tuple(
                capability
                for capability in _INFRASTRUCTURE
                if capability not in provided
            ),
            provides=(
                *(
                    capability
                    for capability in _INFRASTRUCTURE
                    if capability in provided
                ),
                CAP_EVENT_ROUTER,
                CAP_GRAPH_WORKER,
            ),
        )
        self._events = events
        self._tasks = tasks
        self._router_observations = ObservationHub()
        self._worker_observations = ObservationHub()
        self._policies = PolicyRegistry()
        self._executor = executor
        self._execution_factory = execution_factory
        self._provided = provided
        self._owned = (
            CAP_EVENT_BUS in provided and events is None,
            CAP_TASK_BACKEND in provided and tasks is None,
            CAP_GRAPH_EXECUTOR in provided and executor is None,
        )
        self._close_injected = close_injected
        self._history_limit = history_limit
        self._direct_workers = direct_workers
        self.router: EventRouter | None = None
        self.worker: GraphWorker | None = None

    def setup(self, context: PluginContext) -> None:
        """创建或取得底层能力，组装 EventRouter 与 GraphWorker，并向宿主提供本插件声明的能力。

        Args:
            context: 插件宿主的装配上下文，用于取得依赖能力和登记提供的能力。

        Raises:
            TypeError: 外部提供的任务后端不能同时投递与消费 Work，或 Execution 工厂不可调用。
        """

        if CAP_EVENT_BUS in self._provided and self._events is None:
            self._events = memory.EventBus()
        if CAP_TASK_BACKEND in self._provided and self._tasks is None:
            self._tasks = memory.TaskBackend()
        if CAP_GRAPH_EXECUTOR in self._provided and self._executor is None:
            self._executor = Engine(observations=self._worker_observations)
        if CAP_EXECUTION_FACTORY in self._provided and self._execution_factory is None:
            self._execution_factory = Execution
        events = (
            self._events
            if CAP_EVENT_BUS in self._provided
            else context.require(CAP_EVENT_BUS)
        )
        tasks = (
            self._tasks
            if CAP_TASK_BACKEND in self._provided
            else context.require(CAP_TASK_BACKEND)
        )
        if not (
            callable(getattr(tasks, "submit", None))
            and callable(getattr(tasks, "bind", None))
        ):
            raise TypeError("task backend capability must publish and consume Work")
        tasks = cast(TaskBackend, tasks)
        executor = (
            self._executor
            if CAP_GRAPH_EXECUTOR in self._provided
            else context.require(CAP_GRAPH_EXECUTOR)
        )
        execution_factory = (
            self._execution_factory
            if CAP_EXECUTION_FACTORY in self._provided
            else context.require(CAP_EXECUTION_FACTORY)
        )
        if not callable(execution_factory):
            raise TypeError("execution factory capability must be callable")
        self.router = EventRouter(
            events=events,
            publisher=tasks,
            observations=self._router_observations,
        )
        self.worker = GraphWorker(
            consumer=tasks,
            executor=executor,
            execution_factory=cast(ExecutionFactory, execution_factory),
            emit=self.router.publish,
            emit_local=self.router.publish_local,
            observations=self._worker_observations,
            policies=self._policies,
            history_limit=self._history_limit,
            direct_workers=self._direct_workers,
        )
        if CAP_EVENT_BUS in self._provided:
            context.provide(CAP_EVENT_BUS, events)
        if CAP_TASK_BACKEND in self._provided:
            context.provide(CAP_TASK_BACKEND, tasks)
        if CAP_GRAPH_EXECUTOR in self._provided:
            context.provide(CAP_GRAPH_EXECUTOR, executor)
        if CAP_EXECUTION_FACTORY in self._provided:
            context.provide(CAP_EXECUTION_FACTORY, execution_factory)
        context.provide(CAP_EVENT_ROUTER, self.router)
        context.provide(CAP_GRAPH_WORKER, self.worker)

    def start(self, context: PluginContext) -> None:
        """无额外启动步骤；组件已在 setup 中创建。

        Args:
            context: 插件宿主的启动上下文，未使用。
        """

        del context

    def stop(self, context: PluginContext) -> None:
        """先关闭 Worker 与 Router，再逆序关闭本插件拥有或按 close_injected 接管的底层组件。

        Args:
            context: 插件宿主的停止上下文，未使用。

        Raises:
            Exception: 关闭过程中最先出现的失败；其余组件仍会关闭。
        """

        del context
        failure: BaseException | None = None
        if self.worker is not None:
            failure = _close_components((self.worker,), failure)
        if self.router is not None:
            failure = _close_components((self.router,), failure)
        components = tuple(
            component
            for component, owned in zip(
                (self._events, self._tasks, self._executor), self._owned, strict=True
            )
            if component is not None and (owned or self._close_injected)
        )
        failure = _close_components(reversed(_unique(*components)), failure)
        if failure is not None:
            raise failure
