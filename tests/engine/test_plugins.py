"""统一插件宿主和 Runtime 插件装配的契约测试。"""

from __future__ import annotations

import logging
from threading import enumerate as enumerate_threads

import pytest

from interlace import Event, Graph, Node, Output, Ports, Runtime
from interlace.adapters import memory
from interlace.engine.executor import Engine
from interlace.engine.hooks import NodeCall
from interlace.engine.observation import ObservationHub, RuntimeEvent, RuntimeEventKind
from interlace.engine.policies import PolicyRef, PolicyRegistry
from interlace.plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_INPUT_SELECTOR,
    CAP_NODE_HOOK,
    CAP_RUNTIME_OBSERVER,
    CAP_TASK_BACKEND,
    ContributionPlugin,
    NodeHookContribution,
    PluginDescriptor,
    PluginHost,
)
from interlace.runtime import EventRouter, GraphWorker, LocalRuntimePlugin
from interlace.spi import DeliveryResult, Work


def test_multiple_plugins_can_contribute_each_extension_kind() -> None:
    """验证多个插件能分别贡献各类聚合扩展。"""

    observed: list[list[RuntimeEvent]] = [[], []]
    calls: list[str] = []

    def record_call(call: NodeCall) -> NodeCall:
        """记录 Hook 经过的节点 ID，并原样返回调用记录。"""

        calls.append(call.node_id)
        return call

    plugins = tuple(
        ContributionPlugin(
            f"example/{index}",
            selectors={f"example/{index}": AnySelector()},
            hooks={f"example/{index}": record_call},
            observers={
                f"example/{index}": lambda event, index=index: observed[index].append(
                    event
                )
            },
        )
        for index in range(2)
    )
    with Runtime(plugins=plugins) as runtime:
        runtime.register("work", Graph(entrypoint="node").add(node=Source()))
        assert runtime.run("work", "hello") == (Output("hello", "value"),)
        for capability in (CAP_INPUT_SELECTOR, CAP_NODE_HOOK, CAP_RUNTIME_OBSERVER):
            assert runtime.plugin_host is not None
            assert len(runtime.plugin_host.contributions(capability)) == 2
    assert calls == ["node", "node"]
    assert observed[0] == observed[1]
    assert observed[0]


def test_duplicate_contribution_names_across_plugins_are_rejected() -> None:
    """验证跨插件重复的贡献名称被拒绝。"""

    plugins = tuple(
        ContributionPlugin(
            f"example/{index}", observers={"example/shared": lambda event: None}
        )
        for index in range(2)
    )
    with pytest.raises(ValueError, match="already exists"):
        PluginHost(plugins).start()


def test_aggregate_declarations_cannot_be_fulfilled_by_singletons() -> None:
    """验证单例能力不能代替聚合贡献声明。"""

    class WrongKind(RecordingPlugin):
        def setup(self, context):
            """把声明为聚合贡献的 example/items 错误地作为单例能力提供。"""

            context.provide("example/items", object())

    plugin = WrongKind("example/wrong", [])
    plugin.descriptor = PluginDescriptor(
        "example/wrong", "1", contributes=("example/items",)
    )
    with pytest.raises(ValueError, match="did not declare"):
        PluginHost((plugin,)).start()


def test_host_rejects_missing_contribution_and_mixed_capability_kinds() -> None:
    """验证宿主拒绝缺失贡献和混用能力种类。"""

    plugin = RecordingPlugin("example/aggregate", [])
    plugin.descriptor = PluginDescriptor(
        "example/aggregate", "1", contributes=("example/items",)
    )
    with pytest.raises(RuntimeError, match="did not provide declared"):
        PluginHost((plugin,)).start()
    singleton = RecordingPlugin("example/singleton", [], capability="example/items")
    with pytest.raises(ValueError, match="both singleton and aggregate"):
        PluginHost((plugin, singleton))


@pytest.mark.parametrize("stage", ["validation", "setup", "start"])
def test_failed_runtime_construction_does_not_leave_runner_threads(stage) -> None:
    """验证 Runtime 构造失败后不遗留运行器线程。

    Args:
        stage: 构造失败的阶段：依赖校验、插件 setup 或插件 start。
    """

    class Broken(RecordingPlugin):
        def setup(self, context):
            """stage 为 setup 时抛出 RuntimeError，否则按 RecordingPlugin 记录装配。"""

            if stage == "setup":
                raise RuntimeError("setup failed")
            super().setup(context)

        def start(self, context):
            """stage 为 start 时抛出 RuntimeError，否则不做任何事。"""

            if stage == "start":
                raise RuntimeError("start failed")

    plugin = Broken(
        "example/broken",
        [],
        requires=("example/missing",) if stage == "validation" else (),
    )
    before = {
        thread.ident
        for thread in enumerate_threads()
        if thread.name == "interlace-async-runner"
    }
    for _ in range(3):
        with pytest.raises(
            (ValueError, RuntimeError),
            match=r"missing required plugin|setup failed|start failed",
        ):
            Runtime(plugins=(plugin,))
    after = {
        thread.ident
        for thread in enumerate_threads()
        if thread.name == "interlace-async-runner"
    }
    assert after == before


def test_builtin_setup_rolls_back_partially_created_components(monkeypatch) -> None:
    """验证默认插件在任务后端构造失败时关闭已创建的事件总线。"""

    bus = memory.EventBus()
    closed = []
    original_close = bus.close

    def close():
        """记录事件总线被关闭，再调用原始 close。"""

        closed.append(True)
        original_close()

    def broken_tasks():
        """替代 TaskBackend 构造函数，总是抛出 RuntimeError 模拟构造失败。"""

        raise RuntimeError("task construction failed")

    monkeypatch.setattr(bus, "close", close)
    monkeypatch.setattr("interlace.runtime.plugin.memory.EventBus", lambda: bus)
    monkeypatch.setattr("interlace.runtime.plugin.memory.TaskBackend", broken_tasks)
    with pytest.raises(RuntimeError, match="task construction failed"):
        Runtime()
    assert closed == [True]


@pytest.mark.parametrize(
    ("hook", "phase", "error"),
    [(object(), None, TypeError), (lambda call: call, "invalid", ValueError)],
)
def test_hook_contribution_validates_hook_and_phase_before_registration(
    hook, phase, error
) -> None:
    """验证 Hook 贡献注册前检查对象和阶段。

    Args:
        hook: 非 Hook 对象，或合法的单阶段回调。
        phase: 函数 Hook 的执行阶段；非法值应触发 ValueError。
        error: 构造 NodeHookContribution 时期望抛出的异常类型。
    """

    with pytest.raises(error):
        NodeHookContribution(hook, phase=phase, graph="not-yet-registered")


class RecordingPlugin:
    """把 setup/start/stop 按 ``阶段:插件标识`` 记录到共享列表的测试插件。

    Attributes:
        descriptor: 由构造参数生成的插件描述，版本固定为 1.0.0。
        events: 多个插件共享的生命周期记录列表，用于断言调用顺序。
        capability: setup 时以插件标识为值提供的能力；None 表示不提供。
    """

    def __init__(
        self,
        plugin_id: str,
        events: list[str],
        *,
        requires: tuple[str, ...] = (),
        requires_capabilities: tuple[str, ...] = (),
        capability: str | None = None,
    ) -> None:
        """按插件标识和依赖声明构造描述符，并保存共享记录列表。

        Args:
            events: 追加生命周期记录的共享列表。
            capability: 需要在 setup 中提供的能力；None 表示不提供。
        """

        provides = () if capability is None else (capability,)
        self.descriptor = PluginDescriptor(
            plugin_id,
            "1.0.0",
            requires=requires,
            requires_capabilities=requires_capabilities,
            provides=provides,
        )
        self.events = events
        self.capability = capability

    def setup(self, context) -> None:
        """记录 setup，并在声明了能力时以插件标识作为能力值提供。"""

        self.events.append(f"setup:{self.descriptor.id}")
        if self.capability is not None:
            context.provide(self.capability, self.descriptor.id)

    def start(self, context) -> None:
        """记录 start。"""

        del context
        self.events.append(f"start:{self.descriptor.id}")

    def stop(self, context) -> None:
        """记录 stop。"""

        del context
        self.events.append(f"stop:{self.descriptor.id}")


def test_host_orders_dependencies_and_stops_in_reverse() -> None:
    """验证插件按依赖启动并按逆序关闭。"""

    events: list[str] = []
    base = RecordingPlugin("example/base", events, capability="example/base-value")
    feature = RecordingPlugin("example/feature", events, requires=("example/base",))

    host = PluginHost((feature, base)).start()

    assert host.require("example/base-value") == "example/base"
    assert events == [
        "setup:example/base",
        "setup:example/feature",
        "start:example/base",
        "start:example/feature",
    ]
    host.close()
    assert events[-2:] == ["stop:example/feature", "stop:example/base"]


def test_host_orders_capability_dependencies_and_rejects_missing_provider() -> None:
    """验证按能力依赖装配且拒绝缺失的提供者。"""

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


def test_host_rejects_missing_cycle_and_capability_conflict() -> None:
    """验证宿主拒绝缺失依赖、循环依赖和能力冲突。"""

    events: list[str] = []
    with pytest.raises(ValueError, match="missing required plugin"):
        PluginHost(
            (RecordingPlugin("example/feature", events, requires=("example/base",)),)
        )

    left = RecordingPlugin("example/left", events, requires=("example/right",))
    right = RecordingPlugin("example/right", events, requires=("example/left",))
    with pytest.raises(ValueError, match="cyclic plugin dependency"):
        PluginHost((left, right))

    first = RecordingPlugin("example/first", events, capability="example/value")
    second = RecordingPlugin("example/second", events, capability="example/value")
    with pytest.raises(ValueError, match="already provided"):
        PluginHost((first, second)).start()


@pytest.mark.parametrize("api_version", ["1", "3"])
def test_host_rejects_incompatible_plugin_api(api_version: str) -> None:
    """验证宿主同时拒绝旧版和未来的插件 SPI。

    Args:
        api_version: 当前宿主不支持的 SPI 主版本。
    """

    class FuturePlugin:
        """声明不兼容 SPI 版本的最小插件，宿主构造时即应拒绝。

        Attributes:
            descriptor: 携带参数化 api_version 的插件描述。
        """

        descriptor = PluginDescriptor(
            "example/incompatible", "1.0.0", api_version=api_version
        )

        def setup(self, context):
            """满足插件协议的空 setup；版本校验失败后不会被调用。"""

            del context

        def start(self, context):
            """满足插件协议的空 start。"""

            del context

        def stop(self, context):
            """满足插件协议的空 stop。"""

            del context

    with pytest.raises(ValueError, match=f"requires API {api_version}"):
        PluginHost((FuturePlugin(),))


def test_setup_failure_rolls_back_configured_plugins() -> None:
    """验证 setup 失败时回滚已接管的插件。"""

    events: list[str] = []
    base = RecordingPlugin("example/base", events)

    class Broken(RecordingPlugin):
        def setup(self, context) -> None:
            """记录 setup 后抛出 RuntimeError，模拟装配中途失败。"""

            super().setup(context)
            raise RuntimeError("setup failed")

    with pytest.raises(RuntimeError, match="setup failed"):
        PluginHost((base, Broken("example/broken", events))).start()

    assert events[-2:] == ["stop:example/broken", "stop:example/base"]


def test_start_failure_rolls_back_plugins_in_reverse_order() -> None:
    """验证 start 失败时按逆序回滚插件。"""

    events: list[str] = []

    class Broken(RecordingPlugin):
        def start(self, context) -> None:
            """记录 start 后抛出 RuntimeError，模拟启动中途失败。"""

            super().start(context)
            raise RuntimeError("start failed")

    with pytest.raises(RuntimeError, match="start failed"):
        PluginHost(
            (
                RecordingPlugin("example/base", events),
                Broken("example/broken", events),
            )
        ).start()

    assert events[-2:] == ["stop:example/broken", "stop:example/base"]


def test_host_rejects_declared_but_missing_capability() -> None:
    """验证宿主拒绝未兑现的能力声明。"""

    class Missing(RecordingPlugin):
        def setup(self, context) -> None:
            """不提供任何能力，使声明的 example/value 落空。"""

            del context

    plugin = Missing("example/missing", [], capability="example/value")

    with pytest.raises(RuntimeError, match="did not provide declared capabilities"):
        PluginHost((plugin,)).start()


def test_stop_failure_does_not_skip_remaining_plugins() -> None:
    """验证单个插件关闭失败后仍关闭其余插件。"""

    events: list[str] = []

    class BrokenStop(RecordingPlugin):
        def stop(self, context) -> None:
            """记录 stop 后抛出消息含插件标识的 RuntimeError。"""

            super().stop(context)
            raise RuntimeError(f"stop failed: {self.descriptor.id}")

    host = PluginHost(
        (
            BrokenStop("example/first", events),
            BrokenStop("example/second", events),
        )
    ).start()

    with pytest.raises(RuntimeError, match="example/second"):
        host.close()

    assert events[-2:] == ["stop:example/second", "stop:example/first"]


def test_contributions_reject_duplicate_names_and_freeze_after_start() -> None:
    """验证贡献名称唯一且宿主启动后冻结注册。"""

    class Contributions:
        """在 setup 与 start 中直接断言贡献注册规则的插件。

        Attributes:
            descriptor: 声明聚合贡献 example/items 的插件描述。
        """

        descriptor = PluginDescriptor(
            "example/contributions",
            "1.0.0",
            contributes=("example/items",),
        )

        def setup(self, context) -> None:
            """贡献 example/item，并断言同名再次贡献抛出 ValueError。"""

            context.contribute("example/items", "example/item", object())
            with pytest.raises(ValueError, match="already exists"):
                context.contribute("example/items", "example/item", object())

        def start(self, context) -> None:
            """断言宿主启动阶段追加贡献因注册已冻结而抛出 RuntimeError。"""

            with pytest.raises(RuntimeError, match="frozen"):
                context.contribute("example/items", "example/late", object())

        def stop(self, context) -> None:
            """空 stop，本插件没有需要释放的资源。"""

            del context

    PluginHost((Contributions(),)).start().close()


def test_runtime_role_validation_failure_closes_started_plugin_host() -> None:
    """验证角色校验失败时关闭已经启动的插件宿主。"""

    class InvalidRoles:
        """以普通 object 冒充路由与执行角色的插件，触发 Runtime 角色校验失败。

        Attributes:
            descriptor: 声明提供事件路由与 Graph Worker 能力的插件描述。
            stopped: 宿主是否已调用 stop，用于断言校验失败后宿主被关闭。
        """

        descriptor = PluginDescriptor(
            "example/invalid-roles",
            "1.0.0",
            provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER),
        )

        def __init__(self) -> None:
            """初始化为尚未停止。"""

            self.stopped = False

        def setup(self, context) -> None:
            """用普通 object 提供路由与执行角色，使后续角色类型校验失败。"""

            context.provide(CAP_EVENT_ROUTER, object())
            context.provide(CAP_GRAPH_WORKER, object())

        def start(self, context) -> None:
            """空 start；失败发生在宿主启动之后的角色校验。"""

            del context

        def stop(self, context) -> None:
            """标记插件已被宿主停止。"""

            del context
            self.stopped = True

    plugin = InvalidRoles()
    with pytest.raises(TypeError, match="router must implement RouterRole"):
        Runtime(plugins=(plugin,))

    assert plugin.stopped


class AnySelector:
    """选择首个有可用 token 的端口的输入选择器。"""

    def select(self, ports, available, config):
        """按声明顺序返回首个有可用 token 的端口单元组；全部为空时返回 None。"""

        del config
        return next(((port,) for port in ports if available[port]), None)


class Source(Node):
    """把 value 输入原样输出到 value 端口的入口节点。

    Attributes:
        input_ports: 唯一的 str 类型输入端口 value。
        output_ports: 唯一的 str 类型输出端口 value。
    """

    input_ports = Ports(value=str)
    output_ports = Ports(value=str)

    def execute(self, inputs, context):
        """把 value 输入原样输出到 value 端口。"""

        del context
        return Output(inputs["value"], "value")


class Target(Node):
    """使用插件贡献的 example.plugin/any 选择器、原样转发 value 的节点。

    Attributes:
        input_ports: 唯一的 str 类型输入端口 value。
        output_ports: 唯一的 str 类型输出端口 value。
        input_policy: 引用插件贡献的选择器，Graph 冻结时该名称必须已注册。
    """

    input_ports = Ports(value=str)
    output_ports = Ports(value=str)
    input_policy = PolicyRef("example.plugin/any")

    def execute(self, inputs, context):
        """把 value 输入转发到 value 端口；大写转换（如有）由贡献的 exit Hook 完成。"""

        del context
        return Output(inputs["value"], "value")


def test_runtime_installs_extension_contributions_before_graph_freeze() -> None:
    """验证 Runtime 在 Graph 冻结前安装扩展贡献。"""

    plugin = ContributionPlugin(
        "example/plugin",
        selectors={"example.plugin/any": AnySelector()},
        hooks={
            "example.plugin/uppercase": NodeHookContribution(
                lambda call, output: Output(output.value.upper(), output.port),
                phase="exit",
                graph="work.graph",
                node="target",
            )
        },
    )
    graph = (
        Graph(entrypoint="source")
        .add(source=Source(), target=Target())
        .connect("source", "target", source_port="value", target_port="value")
    )

    with Runtime(plugins=(plugin,)) as runtime:
        runtime.register("work.graph", graph)
        assert runtime.run("work.graph", "hello") == (Output("HELLO", "value"),)
        assert runtime.plugin_host is not None
        assert {item.id for item in runtime.plugin_host.descriptors} == {
            "interlace.core/local-runtime",
            "example/plugin",
        }


def test_default_runtime_is_composed_by_the_builtin_plugin() -> None:
    """验证默认 Runtime 经由默认插件装配。"""

    with Runtime() as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.descriptors == (LocalRuntimePlugin().descriptor,)


@pytest.mark.parametrize("scoped", [False, True])
@pytest.mark.parametrize("fail_start", [False, True])
def test_host_releases_contributed_hooks_from_injected_executor(
    scoped: bool, fail_start: bool
) -> None:
    """验证关闭与启动回滚都会卸载注入执行器上的即时及延迟 Hook。

    Args:
        scoped: 是否在 Graph 注册前贡献指定节点的延迟 Hook。
        fail_start: 是否在绑定 Hook 后模拟插件启动失败。
    """

    executor = Engine()
    contributions = ContributionPlugin(
        "example/hooks",
        hooks={
            "example/uppercase": NodeHookContribution(
                lambda call, output: Output(output.value.upper(), output.port),
                phase="exit",
                graph="work" if scoped else None,
                node="node" if scoped else None,
            )
        },
    )

    class RegisterGraph(RecordingPlugin):
        """在启动期间触发延迟 Hook 绑定的测试插件。"""

        def start(self, context) -> None:
            """经 Graph Worker 注册 work，触发延迟 Hook 绑定。

            Raises:
                RuntimeError: fail_start 为真时在注册之后抛出。
            """

            worker = context.require(CAP_GRAPH_WORKER)
            worker.register("work", Graph(entrypoint="node").add(node=Source()))
            if fail_start:
                raise RuntimeError("after hook binding")

    plugins = (
        LocalRuntimePlugin(executor=executor),
        contributions,
        RegisterGraph("example/register", []),
    )
    try:
        if fail_start:
            with pytest.raises(RuntimeError, match="after hook binding"):
                Runtime(plugins=plugins)
        else:
            with Runtime(plugins=plugins) as runtime:
                assert runtime.run("work", "hello") == (Output("HELLO", "value"),)
        with Runtime(plugins=(LocalRuntimePlugin(executor=executor),)) as runtime:
            runtime.register("work", Graph(entrypoint="node").add(node=Source()))
            assert runtime.run("work", "hello") == (Output("hello", "value"),)
    finally:
        executor.close()


def test_standard_contributions_require_runtime_roles() -> None:
    """验证独立宿主不会静默接受缺少消费角色的标准贡献。"""

    plugin = ContributionPlugin(
        "example/observer", observers={"example/trace": lambda event: None}
    )
    with pytest.raises(LookupError, match=r"event-router.*unavailable"):
        PluginHost((plugin,)).start()


def test_host_releases_selectors_from_shared_policy_registry() -> None:
    """验证启动回滚和正常关闭均移除共享注册表中的本次 selector 贡献。"""

    policies = PolicyRegistry()
    tasks = memory.TaskBackend()

    class Roles(RecordingPlugin):
        """通过闭包提供每轮独立角色和共享 selector 注册表。"""

        def setup(self, context) -> None:
            """提供本轮角色，selector 由宿主随后统一安装。"""

            context.provide(CAP_EVENT_ROUTER, router)
            context.provide(CAP_GRAPH_WORKER, worker)

        def start(self, context) -> None:
            """注册依赖贡献选择器的 Graph，使其在本轮冻结。

            Raises:
                RuntimeError: fail_start 为真时在注册之后抛出。
            """

            del context
            worker.register("work", Graph(entrypoint="node").add(node=Target()))
            if fail_start:
                raise RuntimeError("selector consumer failed")

        def stop(self, context) -> None:
            """关闭本轮创建的角色，保留调用方共享的任务后端与注册表。"""

            del context
            worker.close()
            router.close()

    try:
        for fail_start in (True, False):
            router = EventRouter(publisher=tasks)
            worker = GraphWorker(consumer=tasks, policies=policies)
            roles = Roles("example/roles", [])
            roles.descriptor = PluginDescriptor(
                "example/roles", "1", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
            )
            plugin = ContributionPlugin(
                "example/policy", selectors={"example.plugin/any": AnySelector()}
            )
            if fail_start:
                with pytest.raises(RuntimeError, match="selector consumer failed"):
                    Runtime(plugins=(roles, plugin))
            else:
                with Runtime(plugins=(roles, plugin)) as runtime:
                    assert runtime.run("work", "hello") == (Output("hello", "value"),)
            with pytest.raises(ValueError, match="not registered"):
                policies.bind(PolicyRef("example.plugin/any"))
    finally:
        tasks.close()


def test_selector_detach_preserves_frozen_graph_and_later_registration() -> None:
    """验证旧注销函数不删除后续同名注册，已冻结 Graph 保留选择器快照。"""

    policies = PolicyRegistry()
    selector = AnySelector()
    detach = policies.register("example.plugin/any", selector)
    graph = Graph(entrypoint="node").add(node=Target()).freeze(policies)
    detach()
    with pytest.raises(ValueError, match="not registered"):
        policies.bind(PolicyRef("example.plugin/any"))
    replacement = policies.register("example.plugin/any", selector)
    detach()
    assert policies.bind(PolicyRef("example.plugin/any")).selector is selector
    replacement()
    replacement()
    with Runtime() as runtime:
        runtime.register("work", graph)
        assert runtime.run("work", "hello") == (Output("hello", "value"),)


def test_contribution_installation_failure_removes_observer_registrations() -> None:
    """验证中途安装贡献失败时撤销已经安装到调用方观察中心的注册。"""

    observed: list[RuntimeEvent] = []
    router_hub, worker_hub = ObservationHub(), ObservationHub()
    tasks = memory.TaskBackend()
    router = EventRouter(publisher=tasks, observations=router_hub)
    worker = GraphWorker(consumer=tasks, observations=worker_hub)

    class Roles(RecordingPlugin):
        """向宿主提供调用方创建的两个角色。"""

        def setup(self, context) -> None:
            """提供调用方创建、各自挂接独立观察中心的路由与执行角色。"""

            context.provide(CAP_EVENT_ROUTER, router)
            context.provide(CAP_GRAPH_WORKER, worker)

    roles = Roles("example/roles", [])
    roles.descriptor = PluginDescriptor(
        "example/roles", "1", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
    )
    contributions = ContributionPlugin(
        "example/observers",
        observers={"example/first": observed.append, "example/invalid": object()},
    )
    try:
        with pytest.raises(TypeError, match="runtime observer must be callable"):
            Runtime(plugins=(roles, contributions))
        event = RuntimeEvent(RuntimeEventKind.EVENT_PUBLISHED, event_type="example")
        router_hub.publish(event)
        worker_hub.publish(event)
        assert observed == []
    finally:
        worker.close()
        router.close()
        tasks.close()


def test_custom_infrastructure_uses_the_same_local_plugin_path() -> None:
    """验证自定义底层能力使用相同的默认插件装配路径。"""

    events = memory.EventBus()
    tasks = memory.TaskBackend()
    executor = Engine()
    local = LocalRuntimePlugin(events=events, tasks=tasks, executor=executor)

    with Runtime(plugins=(local,)) as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.require("interlace.runtime/event-bus") is events
        assert runtime.plugin_host.require("interlace.runtime/task-backend") is tasks
        assert (
            runtime.plugin_host.require("interlace.runtime/graph-executor") is executor
        )

    # 注入资源仍由调用方持有：Runtime 关闭后仍可订阅、发布、绑定与提交。
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
    executor.close()


def test_runtime_fills_defaults_around_custom_event_bus_capability() -> None:
    """验证替换事件总线后其余能力仍由默认插件补齐。"""

    events: list[str] = []
    bus = memory.EventBus()

    class EventBusPlugin(RecordingPlugin):
        """用调用方创建的内存事件总线替换默认事件总线的插件。"""

        def __init__(self) -> None:
            """以 example/events 标识声明提供 CAP_EVENT_BUS 并共享外层记录列表。"""

            super().__init__(
                "example/events",
                events,
                capability=CAP_EVENT_BUS,
            )

        def setup(self, context) -> None:
            """记录 setup 并提供调用方创建的事件总线。"""

            self.events.append(f"setup:{self.descriptor.id}")
            context.provide(CAP_EVENT_BUS, bus)

        def stop(self, context) -> None:
            """记录 stop 并关闭提供的事件总线。"""

            super().stop(context)
            bus.close()

    with Runtime(plugins=(EventBusPlugin(),)) as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.require(CAP_EVENT_BUS) is bus


@pytest.mark.parametrize(
    ("capability", "component"),
    [
        (CAP_TASK_BACKEND, memory.TaskBackend()),
        (CAP_GRAPH_EXECUTOR, Engine()),
    ],
)
def test_runtime_fills_defaults_around_each_infrastructure_capability(
    capability: str,
    component: memory.EventBus | memory.TaskBackend | Engine,
) -> None:
    """验证独立替换各底层能力时仍能补齐默认装配。

    Args:
        capability: 被替换的底层能力名称。
        component: 替换该能力的组件实例，插件停止时将其关闭。
    """

    class InfrastructurePlugin:
        """只提供被替换能力的插件，其余能力交由默认插件补齐。

        Attributes:
            descriptor: 以能力名末段命名、声明提供该能力的插件描述。
        """

        descriptor = PluginDescriptor(
            f"example/{capability.rsplit('/', 1)[-1]}",
            "1.0.0",
            provides=(capability,),
        )

        def setup(self, context) -> None:
            """提供参数化的替换组件。"""

            context.provide(capability, component)

        def start(self, context) -> None:
            """无启动动作。"""

            del context

        def stop(self, context) -> None:
            """关闭替换组件。"""

            del context
            component.close()

    with Runtime(plugins=(InfrastructurePlugin(),)) as runtime:
        assert runtime.plugin_host is not None
        assert runtime.plugin_host.require(capability) is component


class FailingStop(RecordingPlugin):
    """stop 时抛出带插件标识消息的 RuntimeError。"""

    def stop(self, context) -> None:
        """记录 stop 后总是抛出 RuntimeError。"""

        super().stop(context)
        raise RuntimeError(f"stop {self.descriptor.id} failed")


def _logged_errors(caplog: pytest.LogCaptureFixture) -> list[str]:
    """返回宿主日志中携带异常的消息文本。"""

    return [
        str(record.exc_info[1])
        for record in caplog.records
        if record.name == "interlace.plugins.host" and record.exc_info
    ]


def test_start_rollback_logs_cleanup_failures_and_raises_original(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """启动失败回滚时逆序停止全部插件，清理失败写入日志，重新抛出原始启动失败。"""

    events: list[str] = []

    class FailingStart(RecordingPlugin):
        """start 时失败的插件。"""

        def start(self, context) -> None:
            """记录 start 后总是抛出 RuntimeError。"""

            super().start(context)
            raise RuntimeError("start failed")

    host = PluginHost(
        (
            FailingStop("example/a", events),
            FailingStop("example/b", events),
            FailingStart("example/c", events),
        )
    )
    with (
        caplog.at_level(logging.ERROR, logger="interlace.plugins.host"),
        pytest.raises(RuntimeError, match="start failed"),
    ):
        host.start()

    assert events[-3:] == ["stop:example/c", "stop:example/b", "stop:example/a"]
    assert sorted(_logged_errors(caplog)) == [
        "stop example/a failed",
        "stop example/b failed",
    ]


def test_close_raises_first_cleanup_failure_and_logs_the_rest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """关闭时单个插件失败不跳过后续插件，抛出首个失败并记录其余失败。"""

    events: list[str] = []
    host = PluginHost(
        (FailingStop("example/a", events), FailingStop("example/b", events))
    ).start()
    with (
        caplog.at_level(logging.ERROR, logger="interlace.plugins.host"),
        pytest.raises(RuntimeError, match="stop example/b failed"),
    ):
        host.close()

    assert events[-2:] == ["stop:example/b", "stop:example/a"]
    assert _logged_errors(caplog) == ["stop example/a failed"]
    host.close()


def test_registration_handle_detach_follows_host_phase() -> None:
    """注册句柄在 setup 与关闭阶段可撤销，宿主运行期间撤销失败且注册保持不变。"""

    handles: list = []
    stopped: list[str] = []

    class Registering(RecordingPlugin):
        """setup 中撤销后重新提供，stop 中撤销最终注册。"""

        def setup(self, context) -> None:
            """先提供再重复撤销首个注册，然后提供最终值并保存其句柄。"""

            first = context.provide("example/value", "first")
            first.detach()
            first.detach()
            handles.append(context.provide("example/value", "final"))

        def stop(self, context) -> None:
            """在关闭期间撤销最终注册，并记录本插件已停止。"""

            del context
            handles[0].detach()
            stopped.append(self.descriptor.id)

    plugin = Registering("example/registering", [], capability="example/value")
    host = PluginHost((plugin,)).start()
    assert host.require("example/value") == "final"
    with pytest.raises(RuntimeError, match="while the host runs"):
        handles[0].detach()
    assert host.require("example/value") == "final"

    host.close()

    assert stopped == ["example/registering"]
    with pytest.raises(LookupError, match="unavailable"):
        host.require("example/value")


@pytest.mark.parametrize(
    ("contribution", "message"),
    [
        (
            {"selectors": {"example/any": AnySelector()}},
            "worker must implement PolicyHost",
        ),
        (
            {"hooks": {"example/hook": lambda call: call}},
            "worker must implement HookHost",
        ),
        (
            {"observers": {"example/trace": lambda event: None}},
            "router must implement ObservationHost",
        ),
    ],
)
def test_contribution_installation_checks_the_narrow_role_protocol(
    contribution: dict, message: str
) -> None:
    """每类标准贡献只要求角色实现对应的窄协议，缺失时在启动前失败。

    Args:
        contribution: 传给 ContributionPlugin 的单类贡献。
        message: 期望的协议缺失错误。
    """

    events: list[str] = []

    class Roles(RecordingPlugin):
        """提供不实现任何角色协议的占位角色。"""

        def setup(self, context) -> None:
            """以未实现任何角色协议的 object 提供路由与执行角色。"""

            context.provide(CAP_EVENT_ROUTER, object())
            context.provide(CAP_GRAPH_WORKER, object())

    roles = Roles("example/roles", events)
    roles.descriptor = PluginDescriptor(
        "example/roles", "1", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
    )
    with pytest.raises(TypeError, match=message):
        PluginHost((roles, ContributionPlugin("example/c", **contribution))).start()
    assert events[-1] == "stop:example/roles"


def test_observer_contributions_need_only_the_observation_protocol() -> None:
    """只实现 observe_runtime 的角色即可接收 Observer 贡献，关闭时贡献被注销。"""

    hub = ObservationHub()
    observed: list[RuntimeEvent] = []

    class ObservingRole:
        """只实现 ObservationHost 的角色替身。"""

        def observe_runtime(self, observer):
            """把观察者挂到共享观察中心，并返回其卸载句柄。"""

            return hub.attach(observer)

    class Roles(RecordingPlugin):
        """提供两个只支持观察的角色。"""

        def setup(self, context) -> None:
            """以两个 ObservingRole 分别提供路由与执行角色。"""

            context.provide(CAP_EVENT_ROUTER, ObservingRole())
            context.provide(CAP_GRAPH_WORKER, ObservingRole())

    roles = Roles("example/roles", [])
    roles.descriptor = PluginDescriptor(
        "example/roles", "1", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
    )
    host = PluginHost(
        (
            roles,
            ContributionPlugin("example/c", observers={"example/o": observed.append}),
        )
    ).start()
    event = RuntimeEvent(RuntimeEventKind.EVENT_PUBLISHED, event_type="example")
    hub.publish(event)
    assert observed == [event, event]
    host.close()
    hub.publish(event)
    assert observed == [event, event]
