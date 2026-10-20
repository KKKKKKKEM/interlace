"""插件宿主、扩展接口、单例装饰链与默认装配的契约测试。"""

from __future__ import annotations

import logging
from threading import enumerate as enumerate_threads
from typing import Any

import pytest

from interlace import Event, Graph, Node, Output, Runtime
from interlace.adapters import memory
from interlace.engine.executor import Engine
from interlace.engine import InputPolicy
from interlace.plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_TASK_BACKEND,
    HookBus,
    NoticePoint,
    PluginDescriptor,
    PluginHost,
    plugin,
)
from interlace.plugins.points import (
    NODE_CALL,
    RUNTIME_EVENT,
    NodeCall,
    RuntimeEvent,
    RuntimeEventKind,
)
from interlace.runtime import LocalInfrastructurePlugin, LocalRuntimePlugin
from interlace.spi import DeliveryResult, Work

TRACE: NoticePoint[str] = NoticePoint("example/trace")


class RecordingPlugin:
    """把 setup 与 defer 清理按 ``阶段:插件标识`` 记录到共享列表的测试插件。

    Attributes:
        descriptor: 由构造参数生成的插件描述。
        events: 多个插件共享的生命周期记录列表，用于断言调用顺序。
        capability: setup 时以插件标识为值提供的能力；None 表示不提供。
    """

    def __init__(
        self,
        plugin_id: str,
        events: list[str],
        *,
        requires_capabilities: tuple[str, ...] = (),
        capability: str | None = None,
    ) -> None:
        """按插件标识和依赖声明构造描述符，并保存共享记录列表。

        Args:
            plugin_id: 插件标识。
            events: 追加生命周期记录的共享列表。
            requires_capabilities: 依赖的单例能力。
            capability: 需要在 setup 中提供的能力；None 表示不提供。
        """

        self.descriptor = PluginDescriptor(
            plugin_id,
            requires_capabilities=requires_capabilities,
            provides=() if capability is None else (capability,),
        )
        self.events = events
        self.capability = capability

    def setup(self, api: Any) -> None:
        """记录 setup、登记清理，并在声明了能力时以插件标识作为能力值提供。"""

        self.events.append(f"setup:{self.descriptor.id}")
        api.defer(self.cleanup)
        if self.capability is not None:
            api.provide(self.capability, self.descriptor.id)

    def cleanup(self) -> None:
        """记录清理。"""

        self.events.append(f"cleanup:{self.descriptor.id}")


class FailingCleanup(RecordingPlugin):
    """清理时抛出带插件标识消息的 RuntimeError。"""

    def cleanup(self) -> None:
        """记录清理后总是抛出 RuntimeError。"""

        super().cleanup()
        raise RuntimeError(f"cleanup {self.descriptor.id} failed")


class AnySelector:
    """选择首个有可用 token 的端口的输入选择器。"""

    def select(self, ports, available, config):
        """按声明顺序返回首个有可用 token 的端口单元组；全部为空时返回 None。"""

        del config
        return next(((port,) for port in ports if available[port]), None)


class Source(Node):
    """把 value 输入原样输出到 value 端口的入口节点。

    Attributes:
        inputs: 唯一的 str 类型输入端口 value。
        outputs: 唯一的 str 类型输出端口 value。
    """

    inputs = {"value": str}
    outputs = {"value": str}

    def execute(self, inputs, context):
        """把 value 输入原样输出到 value 端口。"""

        del context
        return Output(inputs["value"], "value")


class Target(Source):
    """使用插件登记的 example.plugin/any 选择器的节点。

    Attributes:
        input_policy: 引用插件登记的选择器，Graph 冻结时该名称必须已登记。
    """

    input_policy = InputPolicy("example.plugin/any")


def uppercase(call: NodeCall, call_next: Any) -> tuple[Output, ...]:
    """把内层输出转为大写的节点中间件。"""

    return tuple(Output(item.value.upper(), item.port) for item in call_next(call))


def _logged_errors(caplog: pytest.LogCaptureFixture) -> list[str]:
    """返回宿主日志中携带异常的消息文本。"""

    return [
        str(record.exc_info[1])
        for record in caplog.records
        if record.name == "interlace.plugins.host" and record.exc_info
    ]


def test_function_and_class_plugins_extend_the_same_runtime() -> None:
    """函数插件与类插件经同一宿主提供 selector、登记节点中间件与观察者，Graph 冻结时可用。"""

    observed: list[Any] = []

    @plugin("example/selectors")
    def selectors(api: Any) -> None:
        """提供输入选择器。"""

        api.provide("example.plugin/any", AnySelector())

    class Features:
        """以类形式登记中间件与观察者的插件。

        Attributes:
            descriptor: 插件声明。
        """

        descriptor = PluginDescriptor("example/features")

        def setup(self, api: Any) -> None:
            """登记节点中间件与生命周期观察者。"""

            api.on(NODE_CALL, uppercase, graph="work.graph", node="target")
            api.on(RUNTIME_EVENT, observed.append, kind="execution.finished")

    graph = (
        Graph(entrypoint="source")
        .add(source=Source(), target=Target())
        .connect("source", "target", source_port="value", target_port="value")
    )
    with Runtime(plugins=(Features(), selectors)) as runtime:
        runtime.register("work.graph", graph)
        assert runtime.run("work.graph", "hello") == (Output("HELLO", "value"),)
        assert [descriptor.id for descriptor in runtime.plugin_host.descriptors] == [
            "interlace.core/local-infrastructure",
            "interlace.core/local-runtime",
            "example/features",
            "example/selectors",
        ]
    assert [event.status for event in observed] == ["succeeded"]


def test_provided_names_conflict_across_plugins() -> None:
    """不同插件提供同一具名值时，宿主启动失败并回滚。"""

    plugins = tuple(
        plugin(f"example/{index}")(
            lambda api: api.provide("example/shared", AnySelector())
        )
        for index in range(2)
    )
    with pytest.raises(ValueError, match="already provided"):
        PluginHost(plugins).start()


def test_plugin_registrations_are_removed_on_close_and_rollback() -> None:
    """插件登记的钩子与具名值在关闭或启动回滚时注销，直接登记的不受影响。"""

    bus = HookBus()
    seen: list[str] = []
    direct = bus.on(TRACE, lambda value: seen.append(f"direct:{value}"))

    @plugin("example/trace")
    def trace(api: Any) -> None:
        """登记观察者与 selector。"""

        api.on(TRACE, lambda value: seen.append(f"plugin:{value}"))
        api.provide("example.plugin/any", AnySelector())

    @plugin("example/broken")
    def broken(api: Any) -> None:
        """在 setup 中失败。"""

        raise RuntimeError("setup failed")

    with pytest.raises(RuntimeError, match="setup failed"):
        PluginHost((trace, broken), hooks=bus).start()
    bus.notify(TRACE, "rollback")
    assert seen == ["direct:rollback"]
    assert dict(bus.provided()) == {}

    host = PluginHost((trace,), hooks=bus).start()
    bus.notify(TRACE, "running")
    host.close()
    bus.notify(TRACE, "closed")
    assert seen == [
        "direct:rollback",
        "direct:running",
        "plugin:running",
        "direct:closed",
    ]
    assert dict(bus.provided()) == {}
    direct.detach()


def test_hooks_and_names_register_anytime_but_capabilities_freeze_after_setup() -> None:
    """钩子与未声明的具名值在 setup 之后仍可登记并随宿主关闭注销；capability 只在 setup 期可提供，
    其他插件声明的 capability 不能被抢先提供。"""

    bus = HookBus()
    seen: list[str] = []
    apis: list[Any] = []

    class Late:
        """保存扩展接口供 setup 之后继续登记的插件。

        Attributes:
            descriptor: 声明提供 example/value 的插件描述。
        """

        descriptor = PluginDescriptor("example/late", provides=("example/value",))

        def setup(self, api: Any) -> None:
            """提供能力并保存扩展接口。"""

            api.provide("example/value", 1)
            apis.append(api)

    @plugin("example/thief")
    def thief(api: Any) -> None:
        """尝试提供其他插件声明的 capability。"""

        with pytest.raises(ValueError, match="declared by"):
            api.provide("example/value", 0)

    host = PluginHost((Late(), thief), hooks=bus).start()
    api = apis[0]
    api.on(TRACE, seen.append)
    api.provide("example/extra", 3)
    with pytest.raises(RuntimeError, match="frozen"):
        api.provide("example/value", 2)
    bus.notify(TRACE, "late")
    assert host.require("example/extra") == 3 and host.require("example/value") == 1
    host.close()
    bus.notify(TRACE, "after")
    assert seen == ["late"]
    assert dict(bus.provided()) == {}


def test_wrappers_are_ordered_after_provider_and_before_consumers() -> None:
    """装饰者在提供者之后、依赖方之前装配；后装配的装饰者位于外层。"""

    events: list[str] = []
    provider = RecordingPlugin("example/provider", events, capability="example/value")

    def wrapper(name: str) -> Any:
        """创建装饰 example/value 的函数插件。"""

        @plugin(f"example/{name}", wraps=("example/value",))
        def wrap(api: Any) -> None:
            """记录并包裹当前实现。"""

            events.append(f"setup:example/{name}")
            api.wrap("example/value", lambda inner: f"{name}({inner})")

        return wrap

    seen: list[Any] = []

    @plugin("example/consumer", requires_capabilities=("example/value",))
    def consumer(api: Any) -> None:
        """记录取得的最终实现。"""

        seen.append(api.require("example/value"))

    host = PluginHost((consumer, wrapper("outer"), provider, wrapper("inner"))).start()
    assert events == [
        "setup:example/provider",
        "setup:example/outer",
        "setup:example/inner",
    ]
    assert seen == ["inner(outer(example/provider))"]
    assert host.require("example/value") == "inner(outer(example/provider))"
    host.close()


def test_wrapper_declarations_are_validated() -> None:
    """装饰无人提供的能力在装配前失败；声明了 wraps 却未装饰在 setup 后失败。"""

    @plugin("example/orphan", wraps=("example/missing",))
    def orphan(api: Any) -> None:
        """装饰不存在的能力。"""

    with pytest.raises(ValueError, match="missing wrapped capability"):
        PluginHost((orphan,))

    provider = RecordingPlugin("example/provider", [], capability="example/value")

    @plugin("example/lazy", wraps=("example/value",))
    def lazy(api: Any) -> None:
        """声明装饰但没有执行。"""

    with pytest.raises(RuntimeError, match="did not fulfil"):
        PluginHost((provider, lazy)).start()

    @plugin("example/undeclared")
    def undeclared(api: Any) -> None:
        """装饰未声明的能力。"""

        api.wrap("example/value", lambda inner: inner)

    with pytest.raises(ValueError, match="did not declare wrapping"):
        PluginHost((provider, undeclared)).start()


@pytest.mark.parametrize("stage", ["validation", "setup"])
def test_failed_runtime_construction_does_not_leave_runner_threads(stage) -> None:
    """Runtime 构造失败后不遗留运行器线程。

    Args:
        stage: 构造失败的阶段：依赖校验或插件 setup。
    """

    class Broken(RecordingPlugin):
        """按阶段失败的插件。"""

        def setup(self, api: Any) -> None:
            """stage 为 setup 时在登记清理后抛出 RuntimeError。"""

            super().setup(api)
            if stage == "setup":
                raise RuntimeError("setup failed")

    broken = Broken(
        "example/broken",
        [],
        requires_capabilities=("example/missing",) if stage == "validation" else (),
    )

    def runner_threads() -> set[int | None]:
        """返回当前异步运行器线程标识。"""

        return {
            thread.ident
            for thread in enumerate_threads()
            if thread.name == "interlace-async-runner"
        }

    before = runner_threads()
    for _ in range(3):
        with pytest.raises(
            (ValueError, RuntimeError),
            match=r"missing required capability|setup failed",
        ):
            Runtime(plugins=(broken,))
    assert runner_threads() == before


def test_builtin_setup_rolls_back_partially_created_components(monkeypatch) -> None:
    """默认基础设施插件在任务后端构造失败时关闭已创建的事件总线。

    Args:
        monkeypatch: 替换默认组件构造函数的夹具。
    """

    bus = memory.EventBus()
    closed = []
    original_close = bus.close

    def close() -> None:
        """记录事件总线被关闭，再调用原始 close。"""

        closed.append(True)
        original_close()

    def broken_tasks() -> None:
        """模拟任务后端构造失败。

        Raises:
            RuntimeError: 总是抛出。
        """

        raise RuntimeError("task construction failed")

    monkeypatch.setattr(bus, "close", close)
    monkeypatch.setattr("interlace.runtime.plugin.memory.EventBus", lambda: bus)
    monkeypatch.setattr("interlace.runtime.plugin.memory.TaskBackend", broken_tasks)
    with pytest.raises(RuntimeError, match="task construction failed"):
        Runtime()
    assert closed == [True]


def test_host_orders_dependencies_and_cleans_up_in_reverse() -> None:
    """插件按依赖 setup，defer 登记的清理按逆序执行。"""

    events: list[str] = []
    base = RecordingPlugin("example/base", events, capability="example/base-value")
    feature = RecordingPlugin(
        "example/feature", events, requires_capabilities=("example/base-value",)
    )

    host = PluginHost((feature, base)).start()

    assert host.require("example/base-value") == "example/base"
    assert events == ["setup:example/base", "setup:example/feature"]
    host.close()
    assert events[-2:] == ["cleanup:example/feature", "cleanup:example/base"]


def test_host_orders_capability_dependencies_and_rejects_missing_provider() -> None:
    """按能力依赖装配，并拒绝缺失的提供者。"""

    events: list[str] = []
    provider = RecordingPlugin("example/provider", events, capability="example/value")
    consumer = RecordingPlugin(
        "example/consumer",
        events,
        requires_capabilities=("example/value",),
    )

    host = PluginHost((consumer, provider)).start()
    assert events[:2] == ["setup:example/provider", "setup:example/consumer"]
    host.close()

    with pytest.raises(ValueError, match="missing required capability"):
        PluginHost((consumer,))


def test_host_rejects_capability_cycle_and_conflict() -> None:
    """宿主拒绝循环的 capability 依赖和能力冲突。"""

    events: list[str] = []
    left = RecordingPlugin(
        "example/left",
        events,
        capability="example/left-value",
        requires_capabilities=("example/right-value",),
    )
    right = RecordingPlugin(
        "example/right",
        events,
        capability="example/right-value",
        requires_capabilities=("example/left-value",),
    )
    with pytest.raises(ValueError, match="cyclic plugin dependency"):
        PluginHost((left, right))

    first = RecordingPlugin("example/first", events, capability="example/value")
    second = RecordingPlugin("example/second", events, capability="example/value")
    with pytest.raises(ValueError, match="already provided"):
        PluginHost((first, second))


@pytest.mark.parametrize("api_version", ["4", "6"])
def test_host_rejects_incompatible_plugin_api(api_version: str) -> None:
    """宿主同时拒绝旧版和未来的扩展 SPI。

    Args:
        api_version: 当前宿主不支持的 SPI 主版本。
    """

    @plugin("example/incompatible")
    def incompatible(api: Any) -> None:
        """版本校验失败后不会被调用。"""

    incompatible.descriptor = PluginDescriptor(
        "example/incompatible", api_version=api_version
    )
    with pytest.raises(ValueError, match=f"requires API {api_version}"):
        PluginHost((incompatible,))


def test_setup_failure_runs_cleanups_registered_before_the_failure() -> None:
    """setup 中途失败时，失败插件已登记的清理与先前插件的清理一起逆序执行。"""

    events: list[str] = []
    base = RecordingPlugin("example/base", events)

    class Broken(RecordingPlugin):
        """登记清理后失败。"""

        def setup(self, api: Any) -> None:
            """登记清理后抛出 RuntimeError，模拟装配中途失败。"""

            super().setup(api)
            raise RuntimeError("setup failed")

    with pytest.raises(RuntimeError, match="setup failed"):
        PluginHost((base, Broken("example/broken", events))).start()

    assert events[-2:] == ["cleanup:example/broken", "cleanup:example/base"]


def test_host_rejects_declared_but_missing_capability_and_rolls_back() -> None:
    """宿主拒绝未兑现的能力声明，并执行已登记的清理。"""

    events: list[str] = []

    class Missing(RecordingPlugin):
        """登记清理但不提供声明的能力。"""

        def setup(self, api: Any) -> None:
            """只登记清理，使声明的 example/value 落空。"""

            api.defer(self.cleanup)

    with pytest.raises(RuntimeError, match="did not fulfil declared capabilities"):
        PluginHost(
            (Missing("example/missing", events, capability="example/value"),)
        ).start()
    assert events == ["cleanup:example/missing"]


def test_runtime_role_validation_failure_closes_started_plugin_host() -> None:
    """角色校验失败时关闭已经启动的插件宿主。"""

    class InvalidRoles:
        """以普通 object 冒充路由与执行角色的插件。

        Attributes:
            descriptor: 声明提供事件路由与 Graph Worker 能力的插件描述。
            stopped: 宿主是否已执行本插件的清理。
        """

        descriptor = PluginDescriptor(
            "example/invalid-roles", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
        )

        def __init__(self) -> None:
            """初始化为尚未清理。"""

            self.stopped = False

        def setup(self, api: Any) -> None:
            """用普通 object 提供路由与执行角色，并登记清理。"""

            api.defer(self.stop)
            api.provide(CAP_EVENT_ROUTER, object())
            api.provide(CAP_GRAPH_WORKER, object())

        def stop(self) -> None:
            """标记插件已被宿主清理。"""

            self.stopped = True

    roles = InvalidRoles()
    with pytest.raises(TypeError, match="router must implement RouterRole"):
        Runtime(plugins=(roles,))

    assert roles.stopped


def test_default_runtime_is_composed_by_the_builtin_plugins() -> None:
    """默认 Runtime 只由两个内建插件装配。"""

    with Runtime() as runtime:
        assert runtime.plugin_host.descriptors == (
            LocalInfrastructurePlugin().descriptor,
            LocalRuntimePlugin().descriptor,
        )


def test_runtime_lifecycle_facts_precede_plugin_cleanup() -> None:
    """runtime.started、runtime.closing、runtime.closed 依次发布，插件清理在 closed 之后执行。"""

    seen: list[str] = []

    def record(event: RuntimeEvent) -> None:
        """记录 Runtime 自身的生命周期事实。"""

        if event.kind.value.startswith("runtime."):
            seen.append(event.kind.value)

    @plugin("example/lifecycle")
    def lifecycle(api: Any) -> None:
        """订阅生命周期事实并登记清理。"""

        api.on(RUNTIME_EVENT, record)
        api.defer(lambda: seen.append("cleanup"))

    runtime = Runtime(plugins=(lifecycle,))
    runtime.close()
    assert seen == [
        RuntimeEventKind.RUNTIME_STARTED.value,
        RuntimeEventKind.RUNTIME_CLOSING.value,
        RuntimeEventKind.RUNTIME_CLOSED.value,
        "cleanup",
    ]


def test_injected_engine_must_share_the_runtime_hook_bus() -> None:
    """注入的 Engine 使用其他钩子总线时装配失败；共享总线时中间件生效。"""

    foreign = Engine()
    try:
        with pytest.raises(TypeError, match="share the Runtime hook bus"):
            Runtime(plugins=(LocalInfrastructurePlugin(executor=foreign),))
    finally:
        foreign.close()

    bus = HookBus()
    bus.on(NODE_CALL, uppercase)
    executor = Engine(hooks=bus)
    try:
        with Runtime(
            plugins=(LocalInfrastructurePlugin(executor=executor),), hooks=bus
        ) as runtime:
            runtime.register("work", Graph(entrypoint="node").add(node=Source()))
            assert runtime.run("work", "hello") == (Output("HELLO", "value"),)
    finally:
        executor.close()


def test_custom_infrastructure_uses_the_same_local_plugin_path() -> None:
    """自定义底层组件经默认基础设施插件装配，Runtime 关闭后仍由调用方持有。"""

    events = memory.EventBus()
    tasks = memory.TaskBackend()
    local = LocalInfrastructurePlugin(events=events, tasks=tasks)

    with Runtime(plugins=(local,)) as runtime:
        assert runtime.plugin_host.require(CAP_EVENT_BUS) is events
        assert runtime.plugin_host.require(CAP_TASK_BACKEND) is tasks

    received: list[str] = []
    events.subscribe("still.open", lambda event: received.append(event.type))
    events.publish(Event("still.open"))
    assert received == ["still.open"]
    unbind = tasks.bind(
        "still.open", lambda delivery: DeliveryResult.ack(), concurrency=1
    )
    tasks.submit("still.open", Work("graph"))
    tasks.wait_idle(1)
    unbind()
    events.close()
    tasks.close()


@pytest.mark.parametrize(
    ("capability", "factory"),
    [
        (CAP_EVENT_BUS, lambda api: memory.EventBus()),
        (CAP_TASK_BACKEND, lambda api: memory.TaskBackend()),
        (CAP_GRAPH_EXECUTOR, lambda api: Engine(hooks=api.hooks)),
    ],
)
def test_runtime_fills_defaults_around_each_infrastructure_capability(
    capability: str, factory: Any
) -> None:
    """独立替换任一底层能力时，其余能力仍由默认插件补齐并可执行 Graph。

    Args:
        capability: 被替换的底层能力名称。
        factory: 以扩展接口创建替换组件的函数。
    """

    created: list[Any] = []

    class Infrastructure:
        """只提供被替换能力的插件。

        Attributes:
            descriptor: 声明提供该能力的插件描述。
        """

        descriptor = PluginDescriptor("example/infrastructure", provides=(capability,))

        def setup(self, api: Any) -> None:
            """提供替换组件并登记关闭。"""

            created.append(factory(api))
            api.defer(created[0].close)
            api.provide(capability, created[0])

    with Runtime(plugins=(Infrastructure(),)) as runtime:
        assert runtime.plugin_host.require(capability) is created[0]
        runtime.register("work", Graph(entrypoint="node").add(node=Source()))
        assert runtime.run("work", "hello") == (Output("hello", "value"),)


def test_setup_rollback_logs_cleanup_failures_and_raises_original(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """setup 失败回滚时逆序执行全部清理，清理失败写入日志，重新抛出原始 setup 失败。

    Args:
        caplog: 日志捕获夹具。
    """

    events: list[str] = []

    class FailingSetup(RecordingPlugin):
        """登记清理后 setup 失败的插件。"""

        def setup(self, api: Any) -> None:
            """登记清理后总是抛出 RuntimeError。"""

            super().setup(api)
            raise RuntimeError("setup failed")

    host = PluginHost(
        (
            FailingCleanup("example/a", events),
            FailingCleanup("example/b", events),
            FailingSetup("example/c", events),
        )
    )
    with (
        caplog.at_level(logging.ERROR, logger="interlace.plugins.host"),
        pytest.raises(RuntimeError, match="setup failed"),
    ):
        host.start()

    assert events[-3:] == [
        "cleanup:example/c",
        "cleanup:example/b",
        "cleanup:example/a",
    ]
    assert sorted(_logged_errors(caplog)) == [
        "cleanup example/a failed",
        "cleanup example/b failed",
    ]


def test_close_raises_first_cleanup_failure_and_logs_the_rest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """关闭时单个清理失败不跳过后续清理，抛出首个失败并记录其余失败。

    Args:
        caplog: 日志捕获夹具。
    """

    events: list[str] = []
    host = PluginHost(
        (FailingCleanup("example/a", events), FailingCleanup("example/b", events))
    ).start()
    with (
        caplog.at_level(logging.ERROR, logger="interlace.plugins.host"),
        pytest.raises(RuntimeError, match="cleanup example/b failed"),
    ):
        host.close()

    assert events[-2:] == ["cleanup:example/b", "cleanup:example/a"]
    assert _logged_errors(caplog) == ["cleanup example/a failed"]
    host.close()


def test_provide_registration_detach_follows_host_phase() -> None:
    """提供句柄在 setup 与关闭阶段可撤销，宿主运行期间撤销失败且提供保持不变。"""

    handles: list[Any] = []
    stopped: list[str] = []

    class Registering(RecordingPlugin):
        """setup 中撤销后重新提供，清理时撤销最终提供。"""

        def setup(self, api: Any) -> None:
            """先提供再重复撤销首个提供，然后提供最终值并登记清理。"""

            first = api.provide("example/value", "first")
            first.detach()
            first.detach()
            handles.append(api.provide("example/value", "final"))
            api.defer(self.cleanup)

        def cleanup(self) -> None:
            """在关闭期间撤销最终提供，并记录本插件已清理。"""

            handles[0].detach()
            stopped.append(self.descriptor.id)

    registering = Registering("example/registering", [], capability="example/value")
    host = PluginHost((registering,)).start()
    assert host.require("example/value") == "final"
    with pytest.raises(RuntimeError, match="while the host runs"):
        handles[0].detach()
    assert host.require("example/value") == "final"

    host.close()

    assert stopped == ["example/registering"]
    with pytest.raises(LookupError, match="unavailable"):
        host.require("example/value")


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"id": "plain"}, ValueError),
        ({"id": " a/b "}, ValueError),
        ({"id": "a/b c"}, ValueError),
        ({"version": "1.x"}, ValueError),
        ({"version": "1.0.0\n"}, ValueError),
        ({"requires_capabilities": ["a/cap"]}, TypeError),
        ({"provides": ("plain",)}, ValueError),
        ({"provides": ("a/cap", "a/cap")}, ValueError),
        ({"provides": ("a/cap",), "wraps": ("a/cap",)}, ValueError),
        ({"provides": ("a/cap",), "requires_capabilities": ("a/cap",)}, ValueError),
        ({"wraps": ("a/cap",), "requires_capabilities": ("a/cap",)}, ValueError),
        ({"api_version": "0"}, ValueError),
    ],
)
def test_plugin_descriptor_rejects_invalid_declarations(kwargs, error) -> None:
    """插件声明在构造时拒绝非法标识、版本、依赖与 capability 组合。

    Args:
        kwargs: 覆盖默认声明的字段。
        error: 期望的异常类型。
    """

    values = {"id": "test/self", **kwargs}
    with pytest.raises(error):
        PluginDescriptor(**values)


def test_runtime_and_host_reject_non_plugins_and_duplicate_ids() -> None:
    """非插件对象报告 TypeError，重复插件标识在装配前失败。"""

    with pytest.raises(TypeError, match="PluginDescriptor"):
        Runtime(plugins=[object()])

    @plugin("test/duplicate")
    def duplicate(api: Any) -> None:
        """空插件。"""

    with pytest.raises(ValueError, match="duplicate plugin"):
        PluginHost([duplicate, duplicate])
