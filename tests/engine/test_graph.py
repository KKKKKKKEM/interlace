"""精简 Graph 定义的契约测试。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Mapping
from dataclasses import replace
from typing import Any, cast

import pytest

from interlace import (
    Context,
    ExecutionPlan,
    Graph,
    InputPolicy,
    Node,
    Output,
    Ports,
    Runtime,
)
from interlace.engine.errors import (
    GraphFrozenError,
    GraphValidationError,
    InvalidOutputError,
)


class Source(Node):
    """产生一个字符串的零输入节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
    """

    input_ports = Ports()
    output_ports = Ports(value=str)
    input_policy = InputPolicy.ON_START

    def execute(self, inputs: Mapping[str, object], context: Context) -> Output:
        """产生固定字符串。

        Args:
            inputs: 空输入。
            context: 当前执行上下文。

        Returns:
            固定字符串 Output。
        """

        del inputs, context
        return Output("value", port="value")


class Sink(Node):
    """接收一个 object 的终端节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=object)
    output_ports = Ports()

    def execute(self, inputs, context: Context) -> None:
        """消费输入。

        Args:
            inputs: 当前字符串输入。
            context: 当前执行上下文。
        """

        del inputs, context


def test_graph_freezes_typed_dag() -> None:
    """派生类型 output 可以连接 object input。"""

    graph = (
        Graph(entrypoint="source")
        .add("source", Source())
        .add("sink", Sink())
        .connect("source", "sink", source_port="value", target_port="value")
        .freeze()
    )

    assert graph.frozen
    assert graph.entrypoint == "source"
    assert len(graph.edges) == 1


def test_graph_rejects_all_node_without_every_required_incoming_port() -> None:
    """ALL Node 的必需端口没有任何入边时应在冻结阶段失败。"""

    class Join(Node):
        """当前契约测试使用的 Join 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(left=str, right=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            del inputs, context

    graph = (
        Graph(entrypoint="source")
        .add(source=Source(), join=Join())
        .connect("source", "join", source_port="value", target_port="left")
    )

    with pytest.raises(GraphValidationError, match="join.*right"):
        graph.freeze()


def test_graph_adds_keyword_node_bindings() -> None:
    """关键字名称直接作为 Graph 内的 Node ID。"""

    source = Source()
    sink = Sink()
    graph = Graph(entrypoint="source").add(source=source, sink=sink)

    assert graph.nodes == {"source": source, "sink": sink}


def test_graph_keyword_add_is_atomic() -> None:
    """批量绑定包含非法 Node 时不留下部分结果。"""

    graph = Graph(entrypoint="source")

    with pytest.raises(TypeError, match="sink.*Node"):
        graph.add(source=Source(), sink=cast(Any, object()))

    assert graph.nodes == {}


def test_graph_add_rejects_mixed_forms() -> None:
    """单 Node 位置参数与关键字批量形式不能混用。"""

    with pytest.raises(TypeError, match="either"):
        cast(Any, Graph().add)("source", Source(), sink=Sink())


def test_graph_accepts_cycle() -> None:
    """普通 typed Edge 可以组成环。"""

    class Relay(Node):
        """当前契约测试使用的 Relay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports(value=str)

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return Output(inputs["value"], port="value")

    graph = Graph(entrypoint="a").add("a", Relay()).add("b", Relay())
    graph.connect("a", "b", source_port="value", target_port="value")
    graph.connect("b", "a", source_port="value", target_port="value")

    assert graph.freeze().frozen
    assert graph.edges[1].target == "a"


def test_graph_rejects_unreachable_node() -> None:
    """冻结时拒绝入口无法到达的定义。"""

    graph = Graph(entrypoint="source")
    graph.add("source", Source()).add("sink", Sink())

    with pytest.raises(GraphValidationError, match="unreachable"):
        graph.freeze()


def test_graph_rejects_incompatible_ports() -> None:
    """宽类型 output 不能连接窄类型 input。"""

    class Wide(Source):
        """当前契约测试使用的 Wide 替代实现。

        Attributes:
            output_ports: 节点声明的输出端口及其类型。
        """

        output_ports = Ports(value=object)

    class Narrow(Sink):
        """当前契约测试使用的 Narrow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
        """

        input_ports = Ports(value=str)

    graph = Graph(entrypoint="source")
    graph.add("source", Wide()).add("sink", Narrow())
    graph.connect("source", "sink", source_port="value", target_port="value")

    with pytest.raises(GraphValidationError, match="incompatible"):
        graph.freeze()


@pytest.mark.parametrize("style", ["sync", "async", "awaitable"])
def test_node_accepts_supported_execute_results(style: str) -> None:
    """统一 Node 支持同步结果、协程方法和同步方法返回 Awaitable。

    Args:
        style: 当前场景使用的 execute 返回方式。
    """

    class Immediate(Node):
        """直接产生同步结果的 Node。"""

        def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
            """同步转换当前输入。

            Args:
                inputs: default 端口携带待转换整数。
                context: 当前执行上下文，本场景不使用。

            Returns:
                加一后的整数输出。
            """

            del context
            return Output(inputs["default"] + 1)

    class Asynchronous(Node):
        """通过协程方法产生相同结果的 Node。"""

        async def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
            """在异步等待之后转换当前输入。

            Args:
                inputs: default 端口携带待转换整数。
                context: 当前执行上下文，本场景不使用。

            Returns:
                加一后的整数输出。
            """

            del context
            await asyncio.sleep(0)
            return Output(inputs["default"] + 1)

    class Deferred(Node):
        """由同步方法返回 Awaitable 的 Node。"""

        def execute(
            self, inputs: Mapping[str, Any], context: Context
        ) -> Awaitable[Output]:
            """返回由 Engine 负责等待的协程结果。

            Args:
                inputs: default 端口携带待转换整数。
                context: 当前执行上下文，本场景不使用。

            Returns:
                完成后得到加一输出的协程。
            """

            del context

            async def produce() -> Output:
                """异步计算本次执行的输出。

                Returns:
                    加一后的整数输出。
                """

                await asyncio.sleep(0)
                return Output(inputs["default"] + 1)

            return produce()

    implementations: dict[str, Node] = {
        "sync": Immediate(),
        "async": Asynchronous(),
        "awaitable": Deferred(),
    }
    graph = Graph(entrypoint="node").add(node=implementations[style]).freeze()
    with Runtime() as runtime:
        runtime.register("unified", graph)
        assert runtime.run("unified", 4) == (Output(5),)


def test_node_rejects_async_generator_result() -> None:
    """统一 Node 不将异步生成器隐式转换成已支持的输出集合。"""

    class Invalid(Node):
        """返回不在当前结果契约内的异步生成器的错误节点。"""

        async def execute(  # type: ignore[override]
            self, inputs: Mapping[str, Any], context: Context
        ) -> AsyncIterator[Output]:
            """产生当前契约明确不支持的异步输出流。

            Args:
                inputs: 当前入口数据，本场景不使用。
                context: 当前执行上下文，本场景不使用。

            Yields:
                不应被执行器接受的异步生成器输出。
            """

            del inputs, context
            yield Output(1)

    graph = Graph(entrypoint="node").add(node=Invalid()).freeze()
    with Runtime() as runtime:
        runtime.register("unsupported", graph)
        with pytest.raises(InvalidOutputError, match="Iterable"):
            runtime.run("unsupported")


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True, "1"])
def test_graph_rejects_invalid_node_timeout(timeout: object) -> None:
    """Node timeout 必须是 None 或有限正数。

    Args:
        timeout: 等待或执行时限，单位秒；None 表示不设置时限。
    """

    source = Source()
    source.timeout = timeout  # type: ignore[assignment]
    graph = Graph(entrypoint="source").add(source=source)

    with pytest.raises(GraphValidationError, match="source.*timeout"):
        graph.freeze()


def test_frozen_graph_is_immutable() -> None:
    """冻结后禁止修改节点和连接。"""

    graph = Graph(entrypoint="source").add("source", Source()).freeze()

    with pytest.raises(GraphFrozenError):
        graph.add("another", Source())


def test_graph_builds_strict_execution_plan() -> None:
    """Plan 仅保留两端都被选中的原始 Edge，并冻结所属 Graph。"""

    graph = (
        Graph(entrypoint="source")
        .add("source", Source())
        .add("left", Sink())
        .add("right", Sink())
        .connect("source", "left", source_port="value", target_port="value")
        .connect("source", "right", source_port="value", target_port="value")
    )

    plan = graph.plan(include={"source", "left"})

    assert isinstance(plan, ExecutionPlan)
    assert graph.frozen
    assert plan.entrypoint == "source"
    assert plan.nodes == frozenset({"source", "left"})
    assert plan.edges == (graph.edges[0],)


def test_execution_plan_constructor_derives_immutable_selection() -> None:
    """直接构造计划时复制节点集合，并派生入口、原始边和路由索引。"""

    graph = (
        Graph(entrypoint="source")
        .add(source=Source(), sink=Sink())
        .connect("source", "sink", source_port="value", target_port="value")
        .freeze()
    )
    selected = {"source", "sink"}
    plan = ExecutionPlan(graph, selected)
    selected.clear()

    assert plan.nodes == frozenset({"source", "sink"})
    assert plan.entrypoint == "source"
    assert plan.edges == graph.edges
    assert plan.outgoing_for("source", "value") == graph.edges


@pytest.mark.parametrize("selected", [set(), {"missing"}, {"source", "missing"}])
def test_execution_plan_replacement_revalidates_selection(selected) -> None:
    """替换节点集合时重新拒绝空计划和未知节点。

    Args:
        selected: 无法形成合法执行计划的节点集合。
    """

    graph = Graph(entrypoint="source").add(source=Source())
    plan = graph.plan(include={"source"})

    with pytest.raises(GraphValidationError):
        replace(plan, nodes=selected)


def test_execution_plan_replacement_rebuilds_edges_before_execution() -> None:
    """替换为合法子集时重新派生边，不沿用原计划的下游缓存。"""

    graph = (
        Graph(entrypoint="source")
        .add(source=Source(), sink=Sink())
        .connect("source", "sink", source_port="value", target_port="value")
    )
    plan = graph.plan(include={"source", "sink"})
    reduced = replace(plan, nodes=frozenset({"source"}))

    assert reduced.edges == ()
    assert reduced.outgoing_for("source", "value") == ()
    with Runtime() as runtime:
        runtime.register("reduced", graph)
        assert runtime.run("reduced", plan=reduced) == (Output("value", "value"),)


@pytest.mark.parametrize(
    ("name", "value"),
    [("entrypoint", "missing"), ("edges", ()), ("_outgoing", {})],
)
def test_execution_plan_rejects_replacement_of_derived_fields(name, value) -> None:
    """派生字段不能独立替换并偏离节点选择。

    Args:
        name: 不允许调用方独立指定的派生字段。
        value: 尝试写入的与原计划不一致的字段值。
    """

    plan = Graph(entrypoint="source").add(source=Source()).plan(include={"source"})

    with pytest.raises((TypeError, ValueError), match="init=False"):
        replace(plan, **{name: value})


def test_execution_plan_constructor_requires_frozen_graph() -> None:
    """直接构造计划时拒绝尚未完成类型和可达性校验的 Graph。"""

    graph = Graph(entrypoint="source").add(source=Source())
    with pytest.raises(GraphValidationError, match="frozen"):
        ExecutionPlan(graph, {"source"})


@pytest.mark.parametrize(
    "selected", [None, "source", b"source", [1], [" "], [["source"]]]
)
def test_execution_plan_constructor_rejects_invalid_node_identifiers(selected) -> None:
    """计划构造入口拒绝字符串容器以及不能作为节点 ID 的集合成员。

    Args:
        selected: 类型或成员不满足节点选择契约的参数。
    """

    graph = Graph(entrypoint="source").add(source=Source()).freeze()
    with pytest.raises(TypeError):
        ExecutionPlan(graph, selected)


def test_execution_plan_rejects_disconnected_selection() -> None:
    """严格 Plan 不会跨过未选中的中间 Node 自动补边。"""

    class Relay(Source):
        """当前契约测试使用的 Relay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports(value=str)
        input_policy = InputPolicy.ALL

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return Output(inputs["value"], "value")

    graph = (
        Graph(entrypoint="source")
        .add("source", Source())
        .add("relay", Relay())
        .add("sink", Sink())
        .connect("source", "relay", source_port="value", target_port="value")
        .connect("relay", "sink", source_port="value", target_port="value")
    )

    with pytest.raises(GraphValidationError, match="unreachable"):
        graph.plan(include={"source", "sink"})
    plan = graph.plan(include={"source", "relay", "sink"})
    with pytest.raises(GraphValidationError, match="unreachable"):
        replace(plan, nodes=frozenset({"source", "sink"}))
    with pytest.raises(GraphValidationError, match="entrypoint"):
        replace(plan, nodes=frozenset({"relay", "sink"}))


def test_execution_plan_requires_all_join_inputs() -> None:
    """裁掉 ALL Node 的任一输入分支时在创建 Plan 阶段失败。"""

    class Split(Node):
        """将同一输入分发到两条计算分支的示例节点。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(left=int, right=int)

        def execute(self, inputs, context: Context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return (
                Output(inputs["value"], "left"),
                Output(inputs["value"], "right"),
            )

    class Relay(Node):
        """当前契约测试使用的 Relay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return Output(inputs["value"], "value")

    class Join(Node):
        """当前契约测试使用的 Join 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports(left=int, right=int)
        output_ports = Ports(total=int)
        input_policy = InputPolicy.ALL

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return Output(inputs["left"] + inputs["right"], "total")

    graph = (
        Graph(entrypoint="split")
        .add("split", Split())
        .add("left", Relay())
        .add("right", Relay())
        .add("join", Join())
        .connect("split", "left", source_port="left", target_port="value")
        .connect("split", "right", source_port="right", target_port="value")
        .connect("left", "join", source_port="value", target_port="left")
        .connect("right", "join", source_port="value", target_port="right")
    )
    with pytest.raises(GraphValidationError, match="ALL node.*right"):
        graph.plan(include={"split", "left", "join"})
    plan = graph.plan(include={"split", "left", "right", "join"})
    with pytest.raises(GraphValidationError, match="ALL node.*right"):
        replace(plan, nodes=frozenset({"split", "left", "join"}))


def test_input_policy_any_uses_declaration_order() -> None:
    """多个就绪端口时，ANY 按声明顺序选择。"""

    class Select(Node):
        """当前契约测试使用的 Select 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports(left=int, right=int, cancel=int)
        input_policy = InputPolicy.ANY

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            return Output(next(iter(inputs)))

    with Runtime() as runtime:
        runtime.register("select", Graph(entrypoint="node").add(node=Select()))
        outputs = runtime.run("select", {"cancel": 3, "right": 2, "left": 1})
    assert outputs == (Output("left"), Output("right"), Output("cancel"))
