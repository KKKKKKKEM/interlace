"""领域 Context 工厂、组合委托和执行隔离契约。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from functools import partial
from threading import Barrier, Event as ThreadEvent
from typing import Any

import pytest

from interlace import Context, Graph, Node, Output, Ports, Runtime, Slot, SlotPool
from interlace.engine.errors import (
    ExecutionCancelledError,
    ExecutionError,
    NodeTimeoutError,
)
from interlace.engine.hooks import NodeCall, NodeHook
from interlace.spi import ContextFactory


class AirContext(Context):
    """组合引擎能力并在工厂内解释业务输入。

    Attributes:
        seed: 当前调用拥有的整数种子，由工厂从输入计算。
    """

    def __init__(self, base: Context, seed: int) -> None:
        """使用领域自己的构造签名，不接收引擎内部绑定参数。

        Args:
            base: 当前调用借用的执行能力。
            seed: 本次调用的业务种子。
        """

        super().__init__(parent=base)
        self.seed = seed

    @classmethod
    def make(cls, base: Context, inputs: Mapping[str, Any]) -> AirContext:
        """通过业务转换初始化种子。

        Args:
            base: 当前调用的基础上下文。
            inputs: Hook enter 前已选中的端口映射。

        Returns:
            具有独立种子字段的组合上下文。
        """

        return cls(base, int(inputs["value"]) + 100)


class ReadSeed(Node):
    """输出工厂初始化的领域种子。"""

    input_ports = Ports(value=int)  # 当前调用的整数触发值。
    output_ports = Ports(value=int)  # 工厂转换后的领域种子。

    def execute(self, inputs, context: Context) -> Output:
        """读取当前领域上下文。

        Args:
            inputs: 原始整数输入。
            context: 工厂返回的组合上下文。

        Returns:
            工厂转换后的种子。
        """

        assert isinstance(context, AirContext)
        context.checkpoint()
        return Output(context.seed, "value")


class AsyncReadSeed(Node):
    """在异步节点中使用相同的领域上下文。"""

    input_ports = Ports(value=int)  # 当前调用的整数触发值。
    output_ports = Ports(value=int)  # 工厂转换后的领域种子。

    async def execute(self, inputs, context: Context) -> Output:
        """在协程切换后读取领域种子。

        Args:
            inputs: 原始整数输入。
            context: 工厂返回的组合上下文。

        Returns:
            工厂转换后的种子。
        """

        await asyncio.sleep(0)
        return ReadSeed().execute(inputs, context)


@pytest.mark.parametrize("method", ["run", "start", "iter", "arun", "aiter"])
@pytest.mark.parametrize("node_type", [ReadSeed, AsyncReadSeed])
def test_context_factory_on_all_entrypoints(method, node_type) -> None:
    """验证同步、异步和输出流均经过领域工厂。

    Args:
        method: 本次检查的公开执行入口。
        node_type: 同步或异步节点类型。
    """

    async def collect(runtime: Runtime) -> tuple[Output, ...]:
        """收集异步入口输出。

        Args:
            runtime: 已注册领域图的运行时。

        Returns:
            本次执行的终端输出。
        """

        if method == "arun":
            return await runtime.arun("air", 3)
        return tuple([output async for output in runtime.aiter("air", 3)])

    graph = Graph(entrypoint="read", context_factory=AirContext.make).add(
        read=node_type()
    )
    with Runtime() as runtime:
        runtime.register("air", graph)
        if method in {"arun", "aiter"}:
            outputs = asyncio.run(collect(runtime))
        else:
            result = getattr(runtime, method)("air", 3)
            outputs = result.result() if method == "start" else tuple(result)
        assert outputs == (Output(103, "value"),)


def test_composition_delegates_capabilities_without_copying_state() -> None:
    """验证组合不复制配置、节点状态、Slot 或静止阶段回调。"""

    events = []
    finalizers = []
    checked = []
    cancelled = False
    slot = Slot({"client": object()})
    base = Context(
        events.append,
        slot,
        checkpoint=lambda: checked.append(True),
        is_cancelled=lambda: cancelled,
        finalizers=finalizers,
        options={"air.rules": []},
    )
    domain = AirContext(base, 7)
    outer = Context(parent=domain)
    assert outer.slot is domain.slot is base.slot
    assert outer.options is domain.options is base.options
    assert outer.state("air") is base.state("air")
    outer.checkpoint()
    assert checked == [True]
    assert not outer.cancelled
    cancelled = True
    assert outer.cancelled
    event = outer.emit("air.seed", 7)
    assert events == [event]
    outer.on_quiescence(lambda: checked.append(False))
    finalizers[0]()
    assert checked == [True, False]
    with pytest.raises(TypeError):
        outer.options["air.rules"] = []
    with pytest.raises(TypeError):
        outer.on_quiescence(None)


def test_context_factory_preserves_node_state_and_firing_isolation() -> None:
    """验证相同节点重入共享命名空间，领域字段和配置按 firing 隔离。"""

    contexts = []
    finalized = []

    class Loop(Node):
        """用回边触发同一节点的三次调用。"""

        input_ports = Ports(value=int)  # 当前循环值。
        output_ports = Ports(value=int, done=int)  # 回边值与终端值。

        def execute(self, inputs, context):
            """验证隔离后继续循环或交付结果。

            Args:
                inputs: 当前循环值。
                context: 本次独立的领域上下文。

            Returns:
                下一次触发值或终端值。
            """

            assert isinstance(context, AirContext)
            value = inputs["value"]
            assert context.seed == value + 100
            state = context.state("air")
            assert state.get("count", 0) == value
            state["count"] = value + 1
            assert context.options["air.rules"] == []
            context.options["air.rules"].append(value)
            context.seed = -1
            contexts.append(context)
            context.on_quiescence(lambda: finalized.append(value))
            return Output(value + 1, "value" if value < 2 else "done")

    graph = (
        Graph(entrypoint="loop", context_factory=AirContext.make)
        .add(loop=Loop())
        .connect("loop", "loop", source_port="value", target_port="value")
    )
    with Runtime() as runtime:
        runtime.register("loop", graph)
        for _ in range(2):
            assert runtime.run("loop", 0, options={"air.rules": []}) == (
                Output(3, "done"),
            )
    assert len({id(context) for context in contexts}) == 6
    assert finalized == [0, 1, 2, 0, 1, 2]


def test_context_factory_and_hooks_share_instance_and_isolate_node_scopes() -> None:
    """验证工厂读取 enter 前输入，Hook 与节点共享实例且不同节点状态隔离。"""

    calls = []

    class Inspect(NodeHook):
        """记录上下文并替换实际节点输入。"""

        def enter(self, call: NodeCall) -> NodeCall:
            """验证领域上下文已经创建。

            Args:
                call: 当前节点的调用信息。

            Returns:
                替换输入后的调用信息。
            """

            assert isinstance(call.context, AirContext)
            assert call.context.state("air") == {}
            call.context.state("air")["entered"] = True
            calls.append(call.context)
            return call.with_inputs({"value": 999})

        def exit(self, call: NodeCall, output: Output) -> Output:
            """验证出口仍然使用同一个上下文。

            Args:
                call: 当前节点调用信息。
                output: 原始节点输出。

            Returns:
                原输出。
            """

            assert call.context is calls[-1]
            assert output.value == call.context.seed
            return output

    graph = (
        Graph(entrypoint="first", context_factory=AirContext.make)
        .add(first=ReadSeed(), second=ReadSeed())
        .connect("first", "second", source_port="value", target_port="value")
    )
    with Runtime() as runtime:
        runtime.register("air", graph)
        runtime.attach(Inspect(), graph="air")
        assert runtime.run("air", 3) == (Output(203, "value"),)
    assert len(calls) == 2
    assert calls[0] is not calls[1]


def test_event_target_uses_own_factory_and_preserves_slot() -> None:
    """验证跨图事件重新创建上下文和配置，但本地延续保留 Slot。"""

    captured = []
    pool = SlotPool(1, factory=lambda: Slot({"identity": "account"}))

    class Publish(Node):
        """将领域种子显式发布给另一个 Graph。"""

        input_ports = Ports(value=int)  # 当前种子。

        def execute(self, inputs, context):
            """保存源上下文并提交后续种子。

            Args:
                inputs: 当前整数种子。
                context: 源图领域上下文。
            """

            assert isinstance(context, AirContext)
            context.state("air")["source"] = True
            captured.append(context)
            context.emit("air.next", context.seed)

    class Collect(Node):
        """检查目标图使用自己的工厂。"""

        input_ports = Ports(value=int)  # 源图发布的种子。

        def execute(self, inputs, context):
            """记录目标上下文并检查隔离。

            Args:
                inputs: 源图发布的种子。
                context: 目标图创建的普通上下文。
            """

            assert type(context) is Context
            assert context.state("air") == {}
            assert context.options == {}
            assert inputs["value"] == 103
            captured.append(context)

    try:
        with Runtime() as runtime:
            runtime.register(
                "source",
                Graph(entrypoint="publish", context_factory=AirContext.make).add(
                    publish=Publish()
                ),
            )
            runtime.register(
                "target", Graph(entrypoint="collect").add(collect=Collect())
            )
            runtime.on("air.start", graph="source", queue="source", slots=pool)
            runtime.on("air.next", graph="target", queue="target")
            runtime.emit("air.start", 3)
            runtime.wait_idle(timeout=5)
    finally:
        pool.close()
    assert len(captured) == 2
    assert captured[0] is not captured[1]
    assert captured[0].slot is captured[1].slot
    assert captured[0].slot is not None


def test_concurrent_executions_do_not_share_domain_context() -> None:
    """验证共享工厂和节点时，两个并发 execution 的领域状态互不覆盖。"""

    barrier = Barrier(2, timeout=5)

    class Wait(ReadSeed):
        """让两个 execution 在同一节点内重叠。"""

        def execute(self, inputs, context):
            """在重叠期间验证当前种子与状态。

            Args:
                inputs: 当前整数种子。
                context: 当前执行独立的领域上下文。

            Returns:
                当前领域种子。
            """

            assert context.state("air") == {}
            context.state("air")["seed"] = context.seed
            barrier.wait()
            assert context.state("air")["seed"] == inputs["value"] + 100
            return super().execute(inputs, context)

    with Runtime() as runtime:
        runtime.register(
            "air",
            Graph(entrypoint="wait", context_factory=AirContext.make).add(wait=Wait()),
        )
        first = runtime.start("air", 1)
        second = runtime.start("air", 2)
        assert first.result(timeout=5) == (Output(101, "value"),)
        assert second.result(timeout=5) == (Output(102, "value"),)


@pytest.mark.parametrize("factory", [None, 1, "AirContext"])
def test_graph_rejects_noncallable_factory(factory) -> None:
    """验证工厂配置在构图时明确校验。

    Args:
        factory: 非法工厂值。
    """

    with pytest.raises(TypeError, match="synchronous callable"):
        Graph(context_factory=factory)


def test_graph_rejects_async_factory() -> None:
    """验证异步工厂不会被误认为同步 Context 创建入口。"""

    async def make(base, inputs):
        """提供用于非法配置测试的异步工厂。

        Args:
            base: 当前执行能力。
            inputs: 当前端口输入。

        Returns:
            原始上下文。
        """

        return base

    with pytest.raises(TypeError, match="synchronous callable"):
        Graph(context_factory=make)
    with pytest.raises(TypeError, match="synchronous callable"):
        Graph(context_factory=partial(make))


@pytest.mark.parametrize("failure", ["raise", "return"])
def test_factory_failure_preserves_cause_and_node(failure) -> None:
    """验证工厂失败保留节点位置，且不会继续调用业务节点。

    Args:
        failure: 工厂抛错或返回非法对象。
    """

    error = ValueError("invalid seed")

    def make(base, inputs):
        """触发指定的工厂失败。

        Args:
            base: 当前执行能力。
            inputs: 当前端口输入。

        Returns:
            用于校验的非法对象。

        Raises:
            ValueError: 本次配置要求抛出原始错误。
        """

        if failure == "raise":
            raise error
        return object()

    with Runtime() as runtime:
        runtime.register(
            "air", Graph(entrypoint="read", context_factory=make).add(read=ReadSeed())
        )
        execution = runtime.start("air", 3)
        with pytest.raises(ExecutionError, match="context factory failed") as raised:
            execution.result(timeout=5)
        assert raised.value.node == "read"
        assert execution.steps == 1
        if failure == "raise":
            assert raised.value.__cause__ is error
        else:
            assert isinstance(raised.value.__cause__, TypeError)


@pytest.mark.parametrize("control", ["cancel", "timeout"])
def test_factory_participates_in_execution_control(control) -> None:
    """验证工厂受取消和节点超时约束，控制错误不被普通失败包装。

    Args:
        control: 本次检查的执行控制类型。
    """

    entered = ThreadEvent()
    release = ThreadEvent()

    def make(base, inputs):
        """等待取消或超时后检查执行边界。

        Args:
            base: 当前执行能力。
            inputs: 当前端口输入。

        Returns:
            正常情况下创建的领域上下文。
        """

        entered.set()
        assert release.wait(5)
        base.checkpoint()
        return AirContext.make(base, inputs)

    class TimedRead(ReadSeed):
        """为工厂与节点共同设置时限。"""

        timeout = 0.02 if control == "timeout" else None  # 本次 firing 的秒数上限。

    with Runtime() as runtime:
        runtime.register(
            "air", Graph(entrypoint="read", context_factory=make).add(read=TimedRead())
        )
        execution = runtime.start("air", 3)
        try:
            assert entered.wait(5)
            if control == "cancel":
                execution.cancel()
            else:
                time.sleep(0.04)
        finally:
            release.set()
        error_type = (
            ExecutionCancelledError if control == "cancel" else NodeTimeoutError
        )
        with pytest.raises(error_type):
            execution.result(timeout=5)


def test_factory_binding_is_readonly_and_parent_rejects_rebinding() -> None:
    """验证 Graph 工厂绑定只读，组合上下文不能同时重新绑定执行能力。"""

    factory: ContextFactory = AirContext.make
    graph = Graph(context_factory=factory)
    assert graph.context_factory is factory
    with pytest.raises(AttributeError):
        graph.context_factory = Context.make
    base = Context(lambda event: None)
    with pytest.raises(TypeError, match="execution bindings"):
        Context(parent=base, options={})
    with pytest.raises(TypeError, match="execution bindings"):
        Context(lambda event: None, parent=base)
    with pytest.raises(TypeError, match="parent must"):
        Context(parent=object())


def test_callable_factory_reads_input_snapshot_with_execution_plan() -> None:
    """验证可调用对象工厂收到只读映射，执行计划保留图的工厂绑定。"""

    class Factory:
        """从端口输入选择领域上下文的无状态工厂。"""

        def __call__(self, base, inputs):
            """校验映射不能修改并创建领域视图。

            Args:
                base: 当前执行能力。
                inputs: 本次选中的输入映射。

            Returns:
                当前调用的领域上下文。
            """

            with pytest.raises(TypeError):
                inputs["value"] = 99
            return AirContext.make(base, inputs)

    graph = Graph(entrypoint="read", context_factory=Factory()).add(read=ReadSeed())
    plan = graph.plan(include=["read"])
    with Runtime() as runtime:
        runtime.register("air", graph)
        assert runtime.run("air", 3, plan=plan) == (Output(103, "value"),)
