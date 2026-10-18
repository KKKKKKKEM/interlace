"""默认单进程 Runtime 的两项内建插件：基础设施与运行角色。

两者与第三方插件经同一 PluginHost 装配；基础设施与角色分离，使其他插件可以通过 ``wraps`` 装饰任一项
基础设施，或只替换 Router、Worker 中的一个。
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

from ..adapters import memory
from ..engine.execution import Execution
from ..engine.executor import Engine
from ..plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_EXECUTION_FACTORY,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_TASK_BACKEND,
    ExtensionAPI,
    PluginDescriptor,
)
from ..spi import (
    EventBus,
    ExecutionFactory,
    GraphExecutor,
    RouterRole,
    TaskBackend,
)
from .router import EventRouter
from .worker import GraphWorker

INFRASTRUCTURE = (
    CAP_EVENT_BUS,
    CAP_TASK_BACKEND,
    CAP_GRAPH_EXECUTOR,
    CAP_EXECUTION_FACTORY,
)  # 默认基础设施能力，按声明顺序排列。
ROLES = (CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)  # 默认运行角色能力。
_ROUTER_NEEDS = (CAP_EVENT_BUS, CAP_TASK_BACKEND)  # 默认 Router 依赖的基础设施。
_WORKER_NEEDS = (
    CAP_TASK_BACKEND,
    CAP_GRAPH_EXECUTOR,
    CAP_EXECUTION_FACTORY,
)  # 默认 Worker 依赖的基础设施。


def required_infrastructure(roles: Iterable[str]) -> frozenset[str]:
    """返回默认角色集合依赖的基础设施能力。

    Args:
        roles: LocalRuntimePlugin 自行提供的角色能力。

    Returns:
        这些角色需要从宿主取得的基础设施能力集合。
    """

    selected = frozenset(roles)
    needs: set[str] = set()
    if CAP_EVENT_ROUTER in selected:
        needs.update(_ROUTER_NEEDS)
    if CAP_GRAPH_WORKER in selected:
        needs.update(_WORKER_NEEDS)
    return frozenset(needs)


def _select(
    values: Iterable[str] | None, allowed: tuple[str, ...], label: str
) -> frozenset[str]:
    """校验所选能力是 allowed 的子集；None 表示全部。

    Raises:
        TypeError: values 是字符串而非能力集合。
        ValueError: 含 allowed 以外的能力。
    """

    if values is None:
        return frozenset(allowed)
    if isinstance(values, str):
        raise TypeError(f"{label} must be an iterable of capabilities")
    selected = frozenset(values)
    if not selected <= frozenset(allowed):
        raise ValueError(f"{label} contains unsupported capabilities")
    return selected


class LocalInfrastructurePlugin:
    """提供默认内存 EventBus、TaskBackend、Engine 与 Execution 工厂的内建插件。

    默认 Engine 使用宿主的钩子总线。注入组件默认由调用方管理，close_injected=True 时由本插件关闭。

    Attributes:
        descriptor: 本插件的身份与所提供能力声明。
        _provided: 本插件提供的基础设施能力集合。
        _events: 注入或在 setup 中创建的事件传输。
        _tasks: 注入或在 setup 中创建的任务后端。
        _executor: 注入或在 setup 中创建的 Graph 执行器。
        _execution_factory: 注入或在 setup 中选用的 Execution 工厂。
        _owned: 依次表示事件传输、任务后端和执行器是否由本插件创建并负责关闭。
        _close_injected: 关闭时是否同时关闭调用方注入的组件。
    """

    def __init__(
        self,
        *,
        events: EventBus | None = None,
        tasks: TaskBackend | None = None,
        executor: GraphExecutor | None = None,
        execution_factory: ExecutionFactory | None = None,
        close_injected: bool = False,
        provides: Iterable[str] | None = None,
    ) -> None:
        """选择提供的基础设施，并保存注入组件与资源所有权设置。

        Args:
            events: 注入的事件传输；None 时在 setup 中创建内存实现。
            tasks: 注入的任务后端，须同时实现 TaskPublisher 与 TaskConsumer；None 时创建内存实现。
            executor: 注入的 Graph 执行器；None 时创建使用宿主钩子总线的默认 Engine。
            execution_factory: 注入的 Execution 工厂；None 时使用 Execution。
            close_injected: 关闭时是否同时关闭注入的 events、tasks 与 executor。
            provides: 本插件提供的基础设施能力子集；None 表示全部四项。

        Raises:
            TypeError: 参数类型不符合契约，或为未提供的能力注入了组件。
            ValueError: provides 含基础设施以外的能力。
        """

        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        if tasks is not None and not (
            callable(getattr(tasks, "submit", None))
            and callable(getattr(tasks, "bind", None))
        ):
            raise TypeError("tasks must implement TaskPublisher and TaskConsumer")
        provided = _select(provides, INFRASTRUCTURE, "provides")
        for capability, injected in (
            (CAP_EVENT_BUS, events),
            (CAP_TASK_BACKEND, tasks),
            (CAP_GRAPH_EXECUTOR, executor),
            (CAP_EXECUTION_FACTORY, execution_factory),
        ):
            if injected is not None and capability not in provided:
                raise TypeError(f"cannot inject {capability!r} without providing it")
        self.descriptor = PluginDescriptor(
            "interlace.core/local-infrastructure",
            provides=tuple(cap for cap in INFRASTRUCTURE if cap in provided),
        )
        self._provided = provided
        self._events = events
        self._tasks = tasks
        self._executor = executor
        self._execution_factory = execution_factory
        self._owned = (events is None, tasks is None, executor is None)
        self._close_injected = close_injected

    def setup(self, api: ExtensionAPI) -> None:
        """创建缺省组件并提供所选基础设施，为本插件负责关闭的组件登记清理。

        Args:
            api: 绑定本插件身份的扩展接口。

        Raises:
            TypeError: 注入的 Engine 没有使用 Runtime 的钩子总线，节点中间件与采集将不会生效。
        """

        closing: list[object] = []

        def own(component: object, owned: bool) -> None:
            """按身份去重登记需要关闭的组件；清理逆序执行。"""

            if (owned or self._close_injected) and all(
                component is not item for item in closing
            ):
                closing.append(component)
                api.defer(cast(Any, component).close)

        if CAP_EVENT_BUS in self._provided:
            if self._events is None:
                self._events = memory.EventBus()
            own(self._events, self._owned[0])
            api.provide(CAP_EVENT_BUS, self._events)
        if CAP_TASK_BACKEND in self._provided:
            if self._tasks is None:
                self._tasks = memory.TaskBackend()
            own(self._tasks, self._owned[1])
            api.provide(CAP_TASK_BACKEND, self._tasks)
        if CAP_GRAPH_EXECUTOR in self._provided:
            if self._executor is None:
                self._executor = Engine(hooks=api.hooks)
            elif (
                isinstance(self._executor, Engine)
                and self._executor.hooks is not api.hooks
            ):
                raise TypeError(
                    "injected Engine must share the Runtime hook bus: "
                    "create Engine(hooks=bus) and Runtime(hooks=bus)"
                )
            own(self._executor, self._owned[2])
            api.provide(CAP_GRAPH_EXECUTOR, self._executor)
        if CAP_EXECUTION_FACTORY in self._provided:
            if self._execution_factory is None:
                self._execution_factory = Execution
            api.provide(CAP_EXECUTION_FACTORY, self._execution_factory)


class LocalRuntimePlugin:
    """以宿主中的基础设施组装默认 EventRouter 与 GraphWorker 的内建插件。

    角色只依赖公开能力：Router 取得 EventBus 与 TaskBackend，Worker 取得 TaskBackend、执行器与 Execution
    工厂；只提供 Worker 时通过 RouterRole 的 publish/publish_local 发布跨图事件。

    Attributes:
        descriptor: 按所选角色生成的身份、依赖与能力声明。
        _roles: 本插件提供的角色能力集合。
        _history_limit: 传给 GraphWorker 的 Execution 记录保留上限。
        _direct_workers: 传给 GraphWorker 的直接执行线程数上限，None 使用标准库默认值。
        router: setup 后创建的默认 Router；未提供或 setup 前为 None。
        worker: setup 后创建的默认 Worker；未提供或 setup 前为 None。
    """

    def __init__(
        self,
        *,
        roles: Iterable[str] | None = None,
        history_limit: int = 1000,
        direct_workers: int | None = None,
    ) -> None:
        """选择提供的角色，并声明它们依赖的基础设施。

        Args:
            roles: 本插件提供的角色能力子集；None 表示 Router 与 Worker 都提供。
            history_limit: 传给 GraphWorker 的 Execution 记录保留上限。
            direct_workers: 传给 GraphWorker 的直接执行线程数上限，None 使用标准库默认值。

        Raises:
            TypeError: roles 是字符串。
            ValueError: roles 为空或含角色以外的能力。
        """

        selected = _select(roles, ROLES, "roles")
        if not selected:
            raise ValueError("roles must select at least one role")
        requires = sorted(required_infrastructure(selected))
        if CAP_GRAPH_WORKER in selected and CAP_EVENT_ROUTER not in selected:
            requires.append(CAP_EVENT_ROUTER)
        self.descriptor = PluginDescriptor(
            "interlace.core/local-runtime",
            provides=tuple(cap for cap in ROLES if cap in selected),
            requires_capabilities=tuple(requires),
        )
        self._roles = selected
        self._history_limit = history_limit
        self._direct_workers = direct_workers
        self.router: EventRouter | None = None
        self.worker: GraphWorker | None = None

    def setup(self, api: ExtensionAPI) -> None:
        """取得基础设施，组装所选角色并提供它们；关闭时先关闭 Worker 再关闭 Router。

        Args:
            api: 绑定本插件身份的扩展接口。

        Raises:
            TypeError: 任务后端不能同时投递与消费 Work，Execution 工厂不可调用，或外部 Router 不满足
                RouterRole。
        """

        tasks: TaskBackend | None = None
        if CAP_TASK_BACKEND in required_infrastructure(self._roles):
            candidate = api.require(CAP_TASK_BACKEND)
            if not (
                callable(getattr(candidate, "submit", None))
                and callable(getattr(candidate, "bind", None))
            ):
                raise TypeError("task backend capability must publish and consume Work")
            tasks = cast(TaskBackend, candidate)
        if CAP_EVENT_ROUTER in self._roles:
            self.router = EventRouter(
                events=api.require(CAP_EVENT_BUS),
                publisher=cast(TaskBackend, tasks),
                hooks=api.hooks,
            )
            api.defer(self.router.close)
            api.provide(CAP_EVENT_ROUTER, self.router)
        if CAP_GRAPH_WORKER in self._roles:
            router = api.require(CAP_EVENT_ROUTER)
            if not isinstance(router, RouterRole):
                raise TypeError("event router capability must implement RouterRole")
            execution_factory = api.require(CAP_EXECUTION_FACTORY)
            if not callable(execution_factory):
                raise TypeError("execution factory capability must be callable")
            self.worker = GraphWorker(
                consumer=cast(TaskBackend, tasks),
                executor=api.require(CAP_GRAPH_EXECUTOR),
                execution_factory=cast(ExecutionFactory, execution_factory),
                emit=router.publish,
                emit_local=router.publish_local,
                hooks=api.hooks,
                history_limit=self._history_limit,
                direct_workers=self._direct_workers,
            )
            api.defer(self.worker.close)
            api.provide(CAP_GRAPH_WORKER, self.worker)
