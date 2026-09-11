"""任意入口、连接裁剪及出口声明的端到端契约。"""

import asyncio
import json
from dataclasses import replace

import pytest

from interlace import Context, Edge, Graph, InputPolicy, Node, Output, Ports, Runtime
from interlace.engine.errors import GraphValidationError, PortValueTypeError


class Origin(Node):
    """原始入口，用不同类型的输出验证计划入口独立绑定。

    Attributes:
        input_ports: 原入口接收字符串。
        output_ports: 向后续节点发送整数。
    """

    input_ports = Ports(text=str)
    output_ports = Ports(value=int)

    def execute(self, inputs, context):
        """将字符串转换为整数。

        Args:
            inputs: 字符串输入。
            context: 本次上下文。

        Returns:
            整数输出。
        """
        return Output(int(inputs["text"]), "value")


class Step(Node):
    """递增后继续或沿重试端口回边。

    Attributes:
        input_ports: 整数输入。
        output_ports: 正常与重试输出。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(value=int, retry=int)

    async def execute(self, inputs, context):
        """等待后选择一个端口，覆盖异步执行与循环。

        Args:
            inputs: 当前整数。
            context: 本次上下文。

        Returns:
            小于二时回边，否则正常输出。
        """
        await asyncio.sleep(0)
        value = inputs["value"] + 1
        return Output(value, "retry" if value < 2 else "value")


class End(Node):
    """回传输入作为原图终端输出。

    Attributes:
        input_ports: 整数输入。
        output_ports: 整数结果。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(value=int)

    def execute(self, inputs, context):
        """原样输出整数。

        Args:
            inputs: 当前整数。
            context: 本次上下文。

        Returns:
            原样结果。
        """
        return Output(inputs["value"], "value")


def workflow():
    """建立带回边和旁路的图。

    Returns:
        尚未冻结的完整 Graph。
    """
    return (
        Graph(entrypoint="origin")
        .add(origin=Origin(), step=Step(), end=End())
        .connect("origin", "step", source_port="value", target_port="value")
        .connect("origin", "end", source_port="value", target_port="value")
        .connect("step", "step", source_port="retry", target_port="value")
        .connect("step", "end", source_port="value", target_port="value")
    )


def test_new_entry_binds_inputs_and_preserves_retry_across_runtime_modes():
    """新的单端口入口在同步、异步及流式入口中使用同一语义。"""
    graph = workflow()
    plan = graph.plan(entrypoint="step", include={"step"}, outputs={("step", "value")})
    assert plan.cut_outputs == {("step", "value")}
    assert plan.edges == (graph.edges[2],)
    with Runtime() as runtime:
        runtime.register("flow", graph)
        assert runtime.run("flow", 0, plan=plan) == (Output(2, "value"),)
        assert asyncio.run(runtime.arun("flow", 0, plan=plan)) == (Output(2, "value"),)
        assert tuple(runtime.iter("flow", 0, plan=plan)) == (Output(2, "value"),)
        with pytest.raises(PortValueTypeError):
            runtime.run("flow", "0", plan=plan)
        assert runtime.run("flow", "2") == (Output(2, "value"), Output(3, "value"))


def test_edge_selection_and_diagnostics_preserve_original_order_and_snapshot():
    """连接选择去掉旁路，诊断同时报告部分扇出裁剪与真正出口。"""
    graph = workflow()
    selected = [graph.edges[3], graph.edges[2], graph.edges[0]]
    plan = graph.plan(include=graph.nodes, edges=selected, outputs={("end", "value")})
    selected.clear()
    assert plan.edges == (graph.edges[0], graph.edges[2], graph.edges[3])
    assert plan.cut_outputs == frozenset()
    assert plan.boundary_edges == (graph.edges[1],)
    description = plan.describe()
    assert description["inputs"] == {"text": "builtins.str"}
    json.dumps(description)
    description["nodes"].clear()
    assert len(plan.describe()["nodes"]) == 3
    with Runtime() as runtime:
        runtime.register("flow", graph)
        assert runtime.run("flow", "0", plan=plan) == (Output(2, "value"),)


def test_cut_retry_requires_explicit_acknowledgement_in_output_boundary():
    """严格出口声明在运行前拒绝意外暴露的重试端口。"""
    graph = workflow()
    with pytest.raises(GraphValidationError, match="unexpected=.*retry"):
        graph.plan(
            include={"step"},
            entrypoint="step",
            edges=[],
            outputs={("step", "value")},
        )
    plan = graph.plan(include={"step"}, entrypoint="step", edges=[])
    assert plan.cut_outputs == {("step", "value"), ("step", "retry")}
    copied = replace(plan)
    assert copied.entrypoint == "step"
    assert copied.edges == ()
    assert copied.describe() == plan.describe()


def test_plan_rejects_new_edges_disconnection_and_invalid_boundary():
    """裁剪不能伪造连接、猜测路径或声明不存在的出口。"""
    graph = workflow()
    with pytest.raises(GraphValidationError, match="original edges"):
        graph.plan(include=graph.nodes, edges=[Edge("end", "step", "value", "value")])
    with pytest.raises(GraphValidationError, match="unreachable"):
        graph.plan(include=graph.nodes, edges=[])
    with pytest.raises(GraphValidationError, match="entrypoint"):
        graph.plan(include={"step"}, entrypoint="end")
    with pytest.raises(TypeError, match="Edge instances"):
        graph.plan(include=graph.nodes, edges=["origin"])
    with pytest.raises(TypeError, match="tuples"):
        graph.plan(include=graph.nodes, outputs=["end.value"])
    with pytest.raises(GraphValidationError, match="missing"):
        graph.plan(include=graph.nodes, outputs={("missing", "value")})


def test_new_multiport_entry_requires_full_input_and_retains_all_validation():
    """新入口可以显式获得多个输入，其他 ALL 节点仍须保留完整上游。"""

    class Join(Node):
        """合并两个整数。

        Attributes:
            input_ports: 两个必需输入。
            output_ports: 求和结果。
            input_policy: 等待所有输入。
        """

        input_ports = Ports(left=int, right=int)
        output_ports = Ports(value=int)
        input_policy = InputPolicy.ALL

        def execute(self, inputs, context: Context):
            """合并输入。

            Args:
                inputs: 左右整数。
                context: 本次上下文。

            Returns:
                两数之和。
            """
            return Output(inputs["left"] + inputs["right"], "value")

    graph = (
        Graph(entrypoint="origin")
        .add(origin=Origin(), join=Join())
        .connect("origin", "join", source_port="value", target_port="left")
        .connect("origin", "join", source_port="value", target_port="right")
    )
    plan = graph.plan(include={"join"}, entrypoint="join")
    with Runtime() as runtime:
        runtime.register("join", graph)
        assert runtime.run("join", {"left": 2, "right": 3}, plan=plan) == (
            Output(5, "value"),
        )
        with pytest.raises(PortValueTypeError):
            runtime.run("join", {"left": 2}, plan=plan)
    with pytest.raises(GraphValidationError, match="ALL"):
        graph.plan(include=graph.nodes, edges=graph.edges[:1])
