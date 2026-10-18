"""Event 驱动 Runtime 的执行契约测试。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Mapping
from threading import Event as ThreadEvent
from threading import Lock, current_thread
from typing import Any, cast

import pytest

from interlace import (
    Context,
    Event,
    ExecutionStatus,
    Graph,
    InputPolicy,
    Node,
    Output,
    Ports,
    Runtime,
    Slot,
    SlotPool,
)
from interlace.adapters import memory
from interlace.engine.errors import (
    IncompleteInputsError,
    InterlaceRuntimeError,
    InvalidOutputError,
    PortValueTypeError,
)
from interlace.engine.observation import RuntimeEvent, RuntimeEventKind
from interlace.runtime import EventRouter, GraphWorker, LocalRuntimePlugin
from interlace.spi import Work


class Split(Node):
    """产生 join 所需的两个图内端口。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(left=int, right=int)

    def execute(self, inputs, context: Context) -> Output | tuple[Output, ...]:
        """把整数复制到两个端口。

        Args:
            inputs: 当前整数输入。
            context: 当前执行上下文。

        Returns:
            两个命名 Output。
        """

        del context
        value = inputs["value"]
        return (Output(value, "left"), Output(value + 1, "right"))


class Join(Node):
    """等待两个输入端口后求和。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
    """

    input_ports = Ports(left=int, right=int)
    output_ports = Ports(total=int)
    input_policy = InputPolicy.ALL

    def execute(self, inputs, context: Context) -> Output:
        """求和并返回终端 Output。

        Args:
            inputs: 同时包含 left 和 right。
            context: 当前执行上下文。

        Returns:
            求和结果。
        """

        del context
        return Output(inputs["left"] + inputs["right"], "total")


def join_graph() -> Graph:
    """构建用于测试图内 Edge 和 InputPolicy 的 Graph。

    Returns:
        已冻结的两节点 Graph。
    """

    return (
        Graph(entrypoint="split")
        .add("split", Split())
        .add("join", Join())
        .connect("split", "join", source_port="left", target_port="left")
        .connect("split", "join", source_port="right", target_port="right")
        .freeze()
    )


def test_runtime_executes_graph_internal_dataflow() -> None:
    """Runtime.run 返回未连接 Edge 的终端 Output。"""

    with Runtime() as runtime:
        runtime.register("join.graph", join_graph())
        outputs = runtime.run("join.graph", 2)

    assert outputs == (Output(5, "total"),)


def test_duplicate_registration_does_not_freeze_rejected_graph() -> None:
    """名称冲突必须在修改调用方 Graph 之前失败。"""

    rejected = Graph(entrypoint="split").add("split", Split())
    with Runtime() as runtime:
        runtime.register("join.graph", join_graph())
        with pytest.raises(InterlaceRuntimeError, match="duplicate registered graph"):
            runtime.register("join.graph", rejected)

    assert not rejected.frozen


def test_on_validates_route_before_creating_consumer() -> None:
    """确定性的 route 参数错误不能留下孤立消费通道。"""

    with Runtime() as runtime:
        with pytest.raises(ValueError, match="event type"):
            runtime.on("", graph="graph", queue="orphan")
        runtime.consume("orphan", concurrency=2)


def test_runtime_executes_self_loop_until_node_stops_emitting_feedback() -> None:
    """自环由 Node 停止产生反馈 Output 后自然结束。"""

    visits: list[int] = []

    class Counter(Node):
        """通过回边递增计数并在达到边界时结束的示例节点。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(again=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            """记录访问值，未到 4 时加一回到 again，否则输出到 done。"""

            del context
            value = inputs["value"]
            visits.append(value)
            if value < 4:
                return Output(value + 1, "again")
            return Output(value, "done")

    graph = (
        Graph(entrypoint="counter")
        .add("counter", Counter())
        .connect("counter", "counter", source_port="again", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("loop.graph", graph)
        outputs = runtime.run("loop.graph", 0)

    assert visits == [0, 1, 2, 3, 4]
    assert outputs == (Output(4, "done"),)


def test_runtime_does_not_impose_cycle_step_limit() -> None:
    """核心不会按 Node 执行次数截断合法长循环。"""

    class Counter(Node):
        """通过回边递增计数并在达到边界时结束的示例节点。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(again=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            """未到 10000 时加一回到 again，否则输出到 done。"""

            del context
            value = inputs["value"]
            if value < 10_000:
                return Output(value + 1, "again")
            return Output(value, "done")

    graph = (
        Graph(entrypoint="counter")
        .add(counter=Counter())
        .connect("counter", "counter", source_port="again", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("long-cycle.graph", graph)
        outputs = runtime.run("long-cycle.graph", 0)

    assert outputs == (Output(10_000, "done"),)


def test_runtime_executes_cycle_across_multiple_nodes() -> None:
    """数据可以多次经过由多个 Node 组成的回路。"""

    calls: list[tuple[str, int]] = []

    class Check(Node):
        """当前契约测试使用的 Check 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(continue_=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            """记录检查值，小于 3 时进入 continue_，否则输出到 done。"""

            del context
            value = inputs["value"]
            calls.append(("check", value))
            if value < 3:
                return Output(value, "continue_")
            return Output(value, "done")

    class Increment(Node):
        """当前契约测试使用的 Increment 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            """记录本次递增，并输出加一后的值。"""

            del context
            value = inputs["value"]
            calls.append(("increment", value))
            return Output(value + 1, "value")

    graph = (
        Graph(entrypoint="check")
        .add(check=Check(), increment=Increment())
        .connect("check", "increment", source_port="continue_", target_port="value")
        .connect("increment", "check", source_port="value", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("cycle.graph", graph)
        outputs = runtime.run("cycle.graph", 0)

    assert calls == [
        ("check", 0),
        ("increment", 0),
        ("check", 1),
        ("increment", 1),
        ("check", 2),
        ("increment", 2),
        ("check", 3),
    ]
    assert outputs == (Output(3, "done"),)


def test_runtime_executes_cycle_inside_execution_plan() -> None:
    """ExecutionPlan 保留所选节点之间的回边。"""

    class Counter(Node):
        """通过回边递增计数并在达到边界时结束的示例节点。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(again=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            """小于 2 时加一回到 again，否则输出到 done。"""

            del context
            value = inputs["value"]
            if value < 2:
                return Output(value + 1, "again")
            return Output(value, "done")

    graph = (
        Graph(entrypoint="counter")
        .add(counter=Counter())
        .connect("counter", "counter", source_port="again", target_port="value")
    )
    plan = graph.plan(include={"counter"})

    with Runtime() as runtime:
        runtime.register("planned-cycle.graph", graph)
        outputs = runtime.run("planned-cycle.graph", 0, plan=plan)

    assert plan.edges == graph.edges
    assert outputs == (Output(2, "done"),)


def test_cycle_scheduler_does_not_starve_ready_branch() -> None:
    """自环每轮只执行一次，已经就绪的旁路会获得调度。"""

    calls: list[str] = []

    class Loop(Node):
        """当前契约测试使用的 Loop 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(again=int, observe=int, done=int)

        def execute(self, inputs, context: Context):
            """小于 3 时同时回到 again 并输出观察值，否则输出到 done。"""

            del context
            value = inputs["value"]
            calls.append(f"loop:{value}")
            if value < 3:
                return (
                    Output(value + 1, "again"),
                    Output(value, "observe"),
                )
            return Output(value, "done")

    class Observe(Node):
        """当前契约测试使用的 Observe 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """记录收到的观察值，不产生输出。"""

            del context
            calls.append(f"observe:{inputs['value']}")

    graph = (
        Graph(entrypoint="loop")
        .add(loop=Loop(), observe=Observe())
        .connect("loop", "loop", source_port="again", target_port="value")
        .connect("loop", "observe", source_port="observe", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("fair.graph", graph)
        outputs = runtime.run("fair.graph", 0)

    assert calls == [
        "loop:0",
        "loop:1",
        "observe:0",
        "loop:2",
        "observe:1",
        "loop:3",
        "observe:2",
    ]
    assert outputs == (Output(3, "done"),)


def test_cycle_quiescence_reports_incomplete_inputs() -> None:
    """回路停止后，无法组成 ALL 输入的残留数据仍是执行错误。"""

    class Loop(Node):
        """当前契约测试使用的 Loop 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(again=int, partial=int, absent=int, done=int)

        def execute(self, inputs, context: Context):
            """小于 2 时回到 again 并产生 partial 输出，否则输出到 done。"""

            del context
            value = inputs["value"]
            if value < 2:
                return (
                    Output(value + 1, "again"),
                    Output(value, "partial"),
                )
            return Output(value, "done")

    class Join(Node):
        """当前契约测试使用的 Join 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports(left=int, right=int)
        output_ports = Ports()
        input_policy = InputPolicy.ALL

        def execute(self, inputs, context: Context) -> None:
            """不产生输出，让对应 ALL 输入保持不完整。"""

            del inputs, context

    graph = (
        Graph(entrypoint="loop")
        .add(loop=Loop(), join=Join())
        .connect("loop", "loop", source_port="again", target_port="value")
        .connect("loop", "join", source_port="partial", target_port="left")
        .connect("loop", "join", source_port="absent", target_port="right")
    )

    with Runtime() as runtime:
        runtime.register("incomplete-cycle.graph", graph)
        with pytest.raises(IncompleteInputsError, match=r"join.left"):
            runtime.run("incomplete-cycle.graph", 0)


def test_runtime_executes_different_plans_from_one_graph() -> None:
    """同一冻结 Graph 可按每次调用选择不同分支，未选 Node 不执行。"""

    calls: list[str] = []

    class SourceNode(Node):
        """当前契约测试使用的 SourceNode 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            """记录调用并原样转发输入值。"""

            del context
            calls.append("source")
            return Output(inputs["value"], "value")

    class Branch(Node):
        """当前契约测试使用的 Branch 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            name: 当前注册项或具名策略的名称。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=str)

        def __init__(self, name: str) -> None:
            """初始化实例及其依赖，建立当前对象独立维护的状态。

            Args:
                name: 注册或查找使用的名称。
            """

            self.name = name

        def execute(self, inputs, context: Context) -> Output:
            """记录分支名，并输出带分支名前缀的结果。"""

            del context
            calls.append(self.name)
            return Output(f"{self.name}:{inputs['value']}", "result")

    graph = (
        Graph(entrypoint="source")
        .add("source", SourceNode())
        .add("fast", Branch("fast"))
        .add("full", Branch("full"))
        .connect("source", "fast", source_port="value", target_port="value")
        .connect("source", "full", source_port="value", target_port="value")
    )
    fast = graph.plan(include={"source", "fast"})
    full = graph.plan(include={"source", "full"})

    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        assert runtime.run("work.graph", 3, plan=fast) == (Output("fast:3", "result"),)
        assert runtime.run("work.graph", 4, plan=full) == (Output("full:4", "result"),)

    assert calls == ["source", "fast", "source", "full"]


def test_runtime_rejects_plan_from_another_graph() -> None:
    """Plan 只能用于创建它的同一个 Graph 实例。"""

    class Start(Node):
        """当前契约测试使用的 Start 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports()
        output_ports = Ports()
        input_policy = InputPolicy.ON_START

        def execute(self, inputs, context: Context) -> None:
            """只作为入口启动，不产生输出。"""

            del inputs, context

    first = Graph(entrypoint="source").add("source", Start()).freeze()
    second = Graph(entrypoint="source").add("source", Start()).freeze()
    plan = first.plan(include={"source"})

    with Runtime() as runtime:
        runtime.register("second", second)
        with pytest.raises(ValueError, match="different Graph"):
            runtime.run("second", plan=plan)


def test_single_port_receives_complete_mapping_payload() -> None:
    """单端口的 Mapping payload 不按多端口输入映射解释。"""

    class MappingConsumer(Node):
        """当前契约测试使用的 MappingConsumer 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(task=dict)
        output_ports = Ports(result=dict)

        def execute(self, inputs, context: Context) -> Output:
            """把映射入口中的 task 值输出为结果。"""

            del context
            return Output(inputs["task"], "result")

    graph = Graph(entrypoint="consume").add("consume", MappingConsumer())
    payload = {"task": 1}
    with Runtime() as runtime:
        runtime.register("mapping.graph", graph)
        outputs = runtime.run("mapping.graph", payload)

    assert outputs == (Output(payload, "result"),)


@pytest.mark.parametrize("node", ["sync", "async"])
def test_runtime_classifies_invalid_node_results(node: str) -> None:
    """同步和异步 Node 的非法返回值都使用公共输出协议错误。"""

    class InvalidSync(Node):
        """当前契约测试使用的 InvalidSync 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports()
        output_ports = Ports()
        input_policy = InputPolicy.ON_START

        def execute(self, inputs, context: Context):
            """违规地同步返回非 Output 值。"""

            del inputs, context
            return cast(Any, 1)  # 验证同步 Node 的非法返回值。

    class InvalidAsync(Node):
        """当前契约测试使用的 InvalidAsync 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            input_policy: 仅依据端口和 token 数量生效的输入策略。
        """

        input_ports = Ports()
        output_ports = Ports()
        input_policy = InputPolicy.ON_START

        async def execute(self, inputs, context: Context):
            """违规地异步返回非 Output 值。"""

            del inputs, context
            return cast(Any, ["not-an-output"])  # 验证异步 Node 的非法返回值。

    invalid = InvalidSync() if node == "sync" else InvalidAsync()
    graph = Graph(entrypoint="invalid").add("invalid", invalid)
    with Runtime() as runtime:
        runtime.register("invalid.graph", graph)
        with pytest.raises(InvalidOutputError) as raised:
            runtime.run("invalid.graph")

    assert raised.value.graph == "invalid.graph"
    assert raised.value.node == "invalid"


def test_idle_runtime_supports_zero_timeout_polling() -> None:
    """零超时是即时空闲检查，空闲 Runtime 应立即成功。"""

    with Runtime() as runtime:
        runtime.wait_idle(0)


def test_context_emit_routes_to_another_graph() -> None:
    """context.emit 通过 route 启动另一张 Graph。"""

    received: list[str] = []

    class Producer(Node):
        """当前契约测试使用的 Producer 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """把输入值作为 value.created 事件发布。"""

            context.emit("value.created", inputs["value"])

    class Consumer(Node):
        """当前契约测试使用的 Consumer 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """记录事件路由过来的值。"""

            del context
            received.append(inputs["value"])

    producer = Graph(entrypoint="produce").add("produce", Producer())
    consumer = Graph(entrypoint="consume").add("consume", Consumer())

    with Runtime() as runtime:
        runtime.register("producer.graph", producer)
        runtime.register("consumer.graph", consumer)
        runtime.on(
            "value.created",
            graph="consumer.graph",
            queue="values",
            concurrency=2,
        )
        runtime.run("producer.graph", "hello")
        runtime.wait_idle()

    assert received == ["hello"]


def test_runtime_keeps_observe_separate_from_combined_on() -> None:
    """observe 只观察 Event，on 组合 route 与 consume。"""

    observed: list[Event] = []
    received: list[str] = []

    class Consumer(Node):
        """当前契约测试使用的 Consumer 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """记录事件路由过来的值。"""

            del context
            received.append(inputs["value"])

    graph = Graph(entrypoint="consume").add("consume", Consumer())
    with Runtime() as runtime:
        runtime.register("consumer.graph", graph)
        runtime.observe("value.observed", observed.append)
        runtime.on(
            "value.routed",
            graph="consumer.graph",
            queue="values",
        )
        runtime.emit("value.observed", "seen")
        runtime.emit("value.routed", "handled")
        runtime.wait_idle()

    assert [event.payload for event in observed] == ["seen"]
    assert received == ["handled"]


def test_runtime_on_forwards_subscription() -> None:
    """组合入口保留 route 的显式 subscription 身份。"""

    class Consumer(Node):
        """当前契约测试使用的 Consumer 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """消费事件但不做处理。"""

            del inputs, context

    graph = Graph(entrypoint="consume").add(consume=Consumer())
    with Runtime() as runtime:
        runtime.register("consumer.graph", graph)
        runtime.on(
            "value.routed",
            graph="consumer.graph",
            queue="values",
            subscription="consumer-values",
        )


def test_runtime_on_does_not_leave_route_when_consumer_setup_fails() -> None:
    """消费配置失败时，同一 route 仍可在修正参数后正常注册。"""

    class Consumer(Node):
        """当前契约测试使用的 Consumer 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """消费事件但不做处理。"""

            del inputs, context

    with Runtime() as runtime:
        runtime.register(
            "consumer.graph",
            Graph(entrypoint="consume").add(consume=Consumer()),
        )
        with pytest.raises(ValueError, match="at least 1"):
            runtime.on(
                "value.routed",
                graph="consumer.graph",
                queue="values",
                concurrency=0,
            )

        runtime.on(
            "value.routed",
            graph="consumer.graph",
            queue="values",
        )


def test_event_is_committed_even_if_source_node_later_fails() -> None:
    """emit 成功后的事件不因当前 Node 后续失败而撤回。"""

    class Broken(Node):
        """当前契约测试使用的 Broken 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """先发布 value.committed 事件，再抛出业务异常。"""

            context.emit("value.committed", inputs["value"])
            raise ValueError("boom")

    graph = Graph(entrypoint="broken").add("broken", Broken())
    seen: list[Event] = []
    with Runtime() as runtime:
        runtime.register("broken.graph", graph)
        runtime.observe("value.committed", seen.append)
        with pytest.raises(Exception, match="boom"):
            runtime.run("broken.graph", "kept")

    assert [event.payload for event in seen] == ["kept"]


def test_runtime_dispatches_each_event_without_domain_deduplication() -> None:
    """Runtime 不判断两个领域 payload 是否代表相同工作。"""

    received: list[str] = []

    class Consumer(Node):
        """当前契约测试使用的 Consumer 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """记录事件路由过来的值。"""

            del context
            received.append(inputs["value"])

    graph = Graph(entrypoint="consume").add("consume", Consumer())
    with Runtime() as runtime:
        runtime.register("consumer.graph", graph)
        runtime.on("value", graph="consumer.graph", queue="values")
        runtime.emit("value", "Same")
        runtime.emit("value", "same")
        runtime.wait_idle()

    assert sorted(received) == ["Same", "same"]


def test_queue_concurrency_limits_graph_executions() -> None:
    """命名执行通道的 concurrency 限制同时运行的 Graph 数量。"""

    active = 0
    maximum = 0
    lock = Lock()

    class Slow(Node):
        """当前契约测试使用的 Slow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
            """统计同时运行的执行数，并异步休眠 20 毫秒。"""

            nonlocal active, maximum
            del inputs, context
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.02)
            finally:
                with lock:
                    active -= 1

    graph = Graph(entrypoint="slow").add("slow", Slow())
    with Runtime() as runtime:
        runtime.register("slow.graph", graph)
        runtime.on(
            "slow.requested",
            graph="slow.graph",
            queue="slow-tasks",
            concurrency=3,
        )
        for value in range(9):
            runtime.emit("slow.requested", value)
        runtime.wait_idle()

    assert maximum == 3


def test_event_can_drive_a_chain_of_graphs() -> None:
    """Event 输出可以继续驱动下一张 Graph。"""

    received: list[str] = []

    class Relay(Node):
        """当前契约测试使用的 Relay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """把输入值作为 relayed 事件转发。"""

            context.emit("relayed", inputs["value"])

    class Sink(Node):
        """当前契约测试使用的 Sink 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """记录事件路由过来的值。"""

            del context
            received.append(inputs["value"])

    graph = Graph(entrypoint="relay").add("relay", Relay())
    sink = Graph(entrypoint="sink").add("sink", Sink())
    with Runtime() as runtime:
        runtime.register("relay.graph", graph)
        runtime.register("sink.graph", sink)
        runtime.on("root", graph="relay.graph", queue="relay-tasks")
        runtime.on("relayed", graph="sink.graph", queue="sink-tasks")
        runtime.emit("root", "value")
        runtime.wait_idle()

    assert received == ["value"]


def test_slot_follows_work_across_consumers() -> None:
    """下游 Worker 沿用上游 Work 的 Slot，而不是从自己的池重新获取。"""

    seen: list[tuple[str, str, str]] = []

    class Producer(Node):
        """当前契约测试使用的 Producer 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """把值写入 Slot，记录 Slot 与线程，再发布 slot.forwarded 事件。"""

            assert context.slot is not None
            context.slot["value"] = inputs["value"]
            seen.append(("source", context.slot.id, current_thread().name))
            context.emit("slot.forwarded", inputs["value"])

    class Consumer(Node):
        """当前契约测试使用的 Consumer 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """确认 Slot 携带源图写入的值，并记录 Slot 与线程。"""

            assert context.slot is not None
            assert context.slot["value"] == inputs["value"]
            seen.append(("sink", context.slot.id, current_thread().name))

    shared = SlotPool(slots=[Slot(id="shared")])
    with Runtime() as runtime:
        runtime.register(
            "source.graph", Graph(entrypoint="source").add(source=Producer())
        )
        runtime.register("sink.graph", Graph(entrypoint="sink").add(sink=Consumer()))
        runtime.on(
            "slot.started",
            graph="source.graph",
            queue="sources",
            concurrency=2,
            slots=shared,
        )
        runtime.on(
            "slot.forwarded",
            graph="sink.graph",
            queue="sinks",
            concurrency=4,
            slots=shared,
        )
        runtime.emit("slot.started", 7)
        runtime.wait_idle()

    assert [(stage, slot_id) for stage, slot_id, _ in seen] == [
        ("source", "shared"),
        ("sink", "shared"),
    ]
    assert seen[0][2].startswith("interlace-sources")
    assert seen[1][2].startswith("interlace-sinks")
    assert shared.available == 1


def test_default_slot_pool_matches_consumer_concurrency() -> None:
    """不传 slots 时自动池限制逻辑执行链数量，并在 Work 后复用 Slot。"""

    slot_ids: list[str] = []
    active = 0
    maximum = 0
    lock = Lock()

    class Capture(Node):
        """当前契约测试使用的 Capture 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
            """记录 Slot ID 并统计同时运行的执行数。"""

            nonlocal active, maximum
            del inputs
            assert context.slot is not None
            with lock:
                slot_ids.append(context.slot.id)
                active += 1
                maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.01)
            finally:
                with lock:
                    active -= 1

    with Runtime() as runtime:
        runtime.register(
            "capture.graph", Graph(entrypoint="capture").add(capture=Capture())
        )
        runtime.on(
            "capture.requested",
            graph="capture.graph",
            queue="captures",
            concurrency=2,
        )
        for value in range(6):
            runtime.emit("capture.requested", value)
        runtime.wait_idle()

    assert maximum == 2
    assert len(set(slot_ids)) == 2


def test_slot_lease_waits_for_all_event_branches() -> None:
    """一个 Work 发出的多个分支共享 Slot，所有分支结束后才归还。"""

    seen: list[tuple[str, str]] = []

    class Fork(Node):
        """当前契约测试使用的 Fork 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """向左右两个分支各发布一个事件。"""

            assert context.slot is not None
            context.emit("branch.left", inputs["value"])
            context.emit("branch.right", inputs["value"])

    class Branch(Node):
        """当前契约测试使用的 Branch 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            name: 当前注册项或具名策略的名称。
        """

        input_ports = Ports(value=str)
        output_ports = Ports()

        def __init__(self, name: str) -> None:
            """初始化实例及其依赖，建立当前对象独立维护的状态。

            Args:
                name: 注册或查找使用的名称。
            """

            self.name = name

        def execute(self, inputs, context: Context) -> None:
            """记录分支名与所用的 Slot ID。"""

            del inputs
            assert context.slot is not None
            seen.append((self.name, context.slot.id))

    shared = SlotPool(slots=[Slot(id="branch-slot")])
    with Runtime() as runtime:
        runtime.register("fork.graph", Graph(entrypoint="fork").add(fork=Fork()))
        runtime.register(
            "left.graph", Graph(entrypoint="left").add(left=Branch("left"))
        )
        runtime.register(
            "right.graph", Graph(entrypoint="right").add(right=Branch("right"))
        )
        runtime.on("fork", graph="fork.graph", queue="forks", slots=shared)
        runtime.on("branch.left", graph="left.graph", queue="left", slots=shared)
        runtime.on("branch.right", graph="right.graph", queue="right", slots=shared)
        runtime.emit("fork", "value")
        runtime.wait_idle()

    assert sorted(seen) == [("left", "branch-slot"), ("right", "branch-slot")]
    assert shared.available == 1


def test_failed_work_releases_its_slot() -> None:
    """Graph 异常不能泄漏 Slot，后续根 Work 仍可获得它。"""

    class Broken(Node):
        """当前契约测试使用的 Broken 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """在携带 Slot 的工作中抛出业务异常。"""

            del inputs, context
            raise ValueError("broken slot work")

    shared = SlotPool(1)
    runtime = Runtime()
    runtime.register("broken.graph", Graph(entrypoint="broken").add(broken=Broken()))
    runtime.on("broken", graph="broken.graph", queue="broken", slots=shared)
    runtime.emit("broken", 1)

    with pytest.raises(Exception, match="broken slot work"):
        runtime.wait_idle()
    assert shared.available == 1
    runtime.close()


def test_rejected_emitted_event_releases_its_retained_slot() -> None:
    """Emitter 拒绝 Node 发出的 Event 时，Work 失败且其 Slot 归还池。"""

    class Emit(Node):
        """发出一个会被 Emitter 拒绝的跨图事件。

        Attributes:
            input_ports: 接收整数入口值的端口。
            output_ports: 无输出端口。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """发出携带入口值的 rejected 事件。

            Args:
                inputs: 包含 value 端口的输入映射。
                context: 当前 Node firing 的上下文。
            """

            context.emit("rejected", inputs["value"])

    def reject(event) -> None:
        """拒绝接管任何事件。

        Args:
            event: Node 发出的事件。

        Raises:
            ValueError: 总是抛出。
        """

        del event
        raise ValueError("rejected event")

    slots = SlotPool(1)
    tasks = memory.TaskBackend()
    worker = GraphWorker(consumer=tasks, emit=reject, close_injected=True)
    worker.register("emit.graph", Graph(entrypoint="emit").add(emit=Emit()))
    worker.consume("emit", slots=slots)
    work = Work("emit.graph", 1)
    tasks.submit("emit", work)

    with pytest.raises(Exception, match="rejected event"):
        worker.wait_idle(2)
    assert worker.get_execution(work.id).status is ExecutionStatus.FAILED
    assert slots.available == 1
    worker.close()
    slots.close()


def test_failed_event_dispatch_releases_forwarded_slot_once() -> None:
    """Work 投递失败后 Router 与 Worker 交接 lease 时不会重复释放。"""

    class Relay(Node):
        """把入口值转发为 rejected 事件。

        Attributes:
            input_ports: 接收整数入口值的端口。
            output_ports: 无输出端口。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """发出携带入口值的 rejected 事件。

            Args:
                inputs: 包含 value 端口的输入映射。
                context: 当前 Node firing 的上下文。
            """

            context.emit("rejected", inputs["value"])

    slots = SlotPool(1)
    runtime = Runtime()
    runtime.register("relay.graph", Graph(entrypoint="relay").add(relay=Relay()))
    # rejected 路由到没有消费者的通道，本地延续投递必然失败。
    runtime.route("rejected", graph="relay.graph", queue="nobody")
    runtime.on("relay", graph="relay.graph", queue="relay", slots=slots)
    runtime.emit("relay", 1)

    with pytest.raises(Exception, match="no active consumers"):
        runtime.wait_idle()
    assert slots.available == 1
    runtime.close()


def test_waiting_roots_do_not_starve_slot_continuations() -> None:
    """根 Work 等 Slot 时不占 Worker，携带 lease 的下游 Work 可继续执行。"""

    received: list[int] = []

    class Relay(Node):
        """当前契约测试使用的 Relay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """把输入值作为 continued 事件转发。"""

            context.emit("continued", inputs["value"])

    class Sink(Node):
        """当前契约测试使用的 Sink 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """记录事件路由过来的值。"""

            del context
            received.append(inputs["value"])

    shared = SlotPool(1)
    with Runtime() as runtime:
        runtime.register("relay.graph", Graph(entrypoint="relay").add(relay=Relay()))
        runtime.register("sink.graph", Graph(entrypoint="sink").add(sink=Sink()))
        runtime.on(
            "root",
            graph="relay.graph",
            queue="roots",
            concurrency=4,
            slots=shared,
        )
        runtime.on(
            "continued",
            graph="sink.graph",
            queue="continuations",
            concurrency=2,
            slots=shared,
        )
        for value in range(8):
            runtime.emit("root", value)
        runtime.wait_idle(2)

    assert sorted(received) == list(range(8))
    assert shared.available == 1


def test_shared_pool_works_when_consumer_and_slot_sizes_differ() -> None:
    """Consumer 并发与共享池大小相互独立，实际链路并发取二者约束的结果。"""

    active = 0
    maximum = 0
    lock = Lock()

    class Slow(Node):
        """当前契约测试使用的 Slow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
            """统计同时持有 Slot 的执行数，并异步休眠 10 毫秒。"""

            nonlocal active, maximum
            del inputs
            assert context.slot is not None
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.01)
            finally:
                with lock:
                    active -= 1

    slots = SlotPool(2)
    with Runtime() as runtime:
        runtime.register("slow.graph", Graph(entrypoint="slow").add(slow=Slow()))
        runtime.on(
            "slow.slot",
            graph="slow.graph",
            queue="slow-slots",
            concurrency=5,
            slots=slots,
        )
        for value in range(8):
            runtime.emit("slow.slot", value)
        runtime.wait_idle()

    assert maximum == 2
    assert slots.available == 2


def test_multiple_routes_retain_one_slot_until_every_work_finishes() -> None:
    """同一 Event 的多个 route 各自接管 lease，最后一个 Work 后才归还。"""

    seen: list[tuple[str, str]] = []

    class FanOut(Node):
        """当前契约测试使用的 FanOut 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """把输入值作为 fanout 事件发布。"""

            context.emit("fanout", inputs["value"])

    class Sink(Node):
        """当前契约测试使用的 Sink 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            name: 当前注册项或具名策略的名称。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def __init__(self, name: str) -> None:
            """初始化实例及其依赖，建立当前对象独立维护的状态。

            Args:
                name: 注册或查找使用的名称。
            """

            self.name = name

        def execute(self, inputs, context: Context) -> None:
            """记录节点名与所用的 Slot ID。"""

            del inputs
            assert context.slot is not None
            seen.append((self.name, context.slot.id))

    slots = SlotPool(slots=[Slot(id="fanout-slot")])
    with Runtime() as runtime:
        runtime.register(
            "fanout.graph", Graph(entrypoint="fanout").add(fanout=FanOut())
        )
        runtime.register("sink-a.graph", Graph(entrypoint="sink").add(sink=Sink("a")))
        runtime.register("sink-b.graph", Graph(entrypoint="sink").add(sink=Sink("b")))
        runtime.on("root.fanout", graph="fanout.graph", queue="fanout", slots=slots)
        runtime.consume("sink-a", slots=slots)
        runtime.consume("sink-b", slots=slots)
        runtime.route("fanout", graph="sink-a.graph", queue="sink-a")
        runtime.route("fanout", graph="sink-b.graph", queue="sink-b")
        runtime.emit("root.fanout", 1)
        runtime.wait_idle()

    assert sorted(seen) == [("a", "fanout-slot"), ("b", "fanout-slot")]
    assert slots.available == 1


def test_queue_rejects_a_different_slot_pool_when_already_bound() -> None:
    """同一 Worker 的同一 queue 不能在重复配置时偷偷切换 SlotPool。"""

    with Runtime() as runtime:
        runtime.consume("queue", concurrency=2, slots=SlotPool(1))
        with pytest.raises(Exception, match="different SlotPool"):
            runtime.consume("queue", concurrency=2, slots=SlotPool(1))


def test_slot_pool_rejects_duplicate_ids() -> None:
    """池内 Slot ID 唯一，便于日志、观测和插件定位逻辑槽。"""

    with pytest.raises(ValueError, match="duplicate Slot ids"):
        SlotPool(slots=[Slot(id="same"), Slot(id="same")])


def test_returned_slot_keeps_state_for_later_root_work() -> None:
    """Slot 归还池时不清空状态，后续 Work 可继续复用其中资源。"""

    counts: list[int] = []

    class Reuse(Node):
        """当前契约测试使用的 Reuse 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """在 Slot 中累加复用次数并记录。"""

            del inputs
            assert context.slot is not None
            context.slot["uses"] = context.slot.get("uses", 0) + 1
            counts.append(context.slot["uses"])

    slots = SlotPool(1)
    with Runtime() as runtime:
        runtime.register("reuse.graph", Graph(entrypoint="reuse").add(reuse=Reuse()))
        runtime.on("reuse", graph="reuse.graph", queue="reuse", slots=slots)
        runtime.emit("reuse", 1)
        runtime.wait_idle()
        runtime.emit("reuse", 2)
        runtime.wait_idle()

    assert counts == [1, 2]
    assert slots.available == 1


def test_runtime_validates_entry_value_type() -> None:
    """入口 payload 必须满足 Ports 声明。"""

    with Runtime() as runtime:
        runtime.register("join.graph", join_graph())
        with pytest.raises(PortValueTypeError):
            runtime.run("join.graph", "not-an-int")


def test_runtime_reports_incomplete_join() -> None:
    """图停止时残留的半组输入必须显式失败。"""

    class LeftOnly(Split):
        def execute(self, inputs, context: Context):
            """只输出到 left 端口。"""

            del context
            return Output(inputs["value"], "left")

    graph = (
        Graph(entrypoint="split")
        .add("split", LeftOnly())
        .add("join", Join())
        .connect("split", "join", source_port="left", target_port="left")
        .connect("split", "join", source_port="right", target_port="right")
    )
    # 两个必需端口都有 Edge，但本次 Node firing 只产生 left。
    graph.freeze()
    with Runtime() as runtime:
        runtime.register("incomplete.graph", graph)
        with pytest.raises(IncompleteInputsError):
            runtime.run("incomplete.graph", 1)


def test_mixed_node_styles_preserve_outputs_events_options_and_slot() -> None:
    """同图混合同步和异步方法，共享数据流和 Slot，并保留跨图配置隔离。"""

    seen_slots: list[Slot] = []
    seen_options: list[dict[str, Any]] = []

    def record(stage: str, context: Context) -> None:
        """记录节点上下文，并验证本次 firing 的配置是独立副本。

        Args:
            stage: 当前节点在逻辑执行链中的名称。
            context: 当前节点得到的配置和 Slot 上下文。
        """

        assert context.options == {"pipeline.rules": ["initial"]}
        assert context.slot is not None
        seen_options.append({"pipeline.rules": list(context.options["pipeline.rules"])})
        context.options["pipeline.rules"].append(stage)
        context.slot.setdefault("stages", []).append(stage)
        seen_slots.append(context.slot)

    class Immediate(Node):
        """执行混合图第一步的同步节点。"""

        input_ports = Ports(value=int)  # 当前图的整数入口。
        output_ports = Ports(value=int)  # 发往异步节点的整数输出。

        def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
            """记录同步阶段并沿 Edge 传递加一结果。

            Args:
                inputs: value 端口的当前整数。
                context: 当前 execution 的隔离配置和共享 Slot。

            Returns:
                加一后的整数输出。
            """

            record("sync", context)
            return Output(inputs["value"] + 1, "value")

    class Asynchronous(Node):
        """通过协程方法执行混合图第二步的节点。"""

        input_ports = Ports(value=int)  # 上游同步节点的整数结果。
        output_ports = Ports(value=int)  # 发往 Awaitable 节点的整数输出。

        async def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
            """等待后记录异步阶段，并继续使用相同数据流契约。

            Args:
                inputs: value 端口的上游整数。
                context: 当前 execution 的隔离配置和共享 Slot。

            Returns:
                扩大两倍后的整数输出。
            """

            await asyncio.sleep(0)
            record("async", context)
            return Output(inputs["value"] * 2, "value")

    class Deferred(Node):
        """由同步方法返回协程，完成原图输出并发布跨图事件。"""

        input_ports = Ports(value=int)  # 上游协程节点的整数结果。
        output_ports = Ports(value=int)  # 无下游 Edge 的原图终端结果。

        def execute(
            self, inputs: Mapping[str, Any], context: Context
        ) -> Awaitable[Output]:
            """构造由 Engine 等待的本次异步处理协程。

            Args:
                inputs: value 端口的上游整数。
                context: 当前 execution 的隔离配置和共享 Slot。

            Returns:
                发布事件并返回终端输出的协程。
            """

            async def complete() -> Output:
                """在 Awaitable 完成前显式发布独立的跨图事实。

                Returns:
                    原图最终的加一输出。
                """

                await asyncio.sleep(0)
                record("awaitable", context)
                value = inputs["value"] + 1
                context.emit("mixed.completed", value)
                return Output(value, "value")

            return complete()

    class FollowUp(Node):
        """在第二张图中验证事件输入、空配置和同一个 Slot。"""

        input_ports = Ports(value=int)  # 已接受跨图事件的整数 payload。
        output_ports = Ports(value=int)  # 第二张图独立产生的终端结果。

        async def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
            """继续原 Slot 逻辑链，但使用新的 execution 配置。

            Args:
                inputs: value 端口接收的事件 payload。
                context: 目标图自己的配置和延续自源图的 Slot。

            Returns:
                原样交付的事件输入。
            """

            await asyncio.sleep(0)
            assert context.options == {}
            assert context.slot is not None
            assert context.slot["stages"] == ["sync", "async", "awaitable"]
            context.slot["stages"].append("target")
            seen_slots.append(context.slot)
            return Output(inputs["value"], "value")

    graph = (
        Graph(entrypoint="first")
        .add(first=Immediate(), second=Asynchronous(), third=Deferred())
        .connect("first", "second", source_port="value", target_port="value")
        .connect("second", "third", source_port="value", target_port="value")
    )
    tasks = memory.TaskBackend()
    slots = SlotPool(1)
    options = {"pipeline.rules": ["initial"]}
    work = Work("mixed", 3, options=options)
    options["pipeline.rules"].append("caller change")
    try:
        with Runtime(plugins=[LocalRuntimePlugin(tasks=tasks)]) as runtime:
            runtime.register("mixed", graph)
            runtime.register("follow", Graph(entrypoint="node").add(node=FollowUp()))
            runtime.consume("source", slots=slots)
            runtime.on("mixed.completed", graph="follow", queue="target", slots=slots)
            tasks.submit("source", work)
            runtime.wait_idle(2)
            source = next(
                item for item in runtime.executions() if item.graph == "mixed"
            )
            target = next(
                item for item in runtime.executions() if item.graph == "follow"
            )
            assert source.result() == target.result() == (Output(9, "value"),)
            assert source.steps == 3
            assert target.steps == 1
            assert slots.available == 1
    finally:
        tasks.close()
        slots.close()

    assert seen_options == [{"pipeline.rules": ["initial"]}] * 3
    assert len(seen_slots) == 4
    assert all(slot is seen_slots[0] for slot in seen_slots)


class Gate(Node):
    """首次执行等待测试放行，可按需让第一次执行失败。

    Attributes:
        entered: 首次执行开始时设置的线程事件。
        release: 放行首次执行的线程事件。
        fail_first: 首次执行放行后是否抛出异常。
        calls: 已开始的执行次数，仅供本实例的测试读取。
    """

    def __init__(self, *, fail_first: bool = False) -> None:
        """创建尚未执行的闸门节点。

        Args:
            fail_first: 首次执行放行后是否抛出 ValueError。
        """

        self.entered = ThreadEvent()
        self.release = ThreadEvent()
        self.fail_first = fail_first
        self.calls = 0

    def execute(self, inputs, context: Context) -> None:
        """首次执行阻塞到放行，之后的执行立即完成。

        Args:
            inputs: 未使用的入口数据。
            context: 未使用的执行上下文。

        Raises:
            ValueError: fail_first 为 True 时的首次执行。
        """

        del inputs, context
        self.calls += 1
        if self.calls > 1:
            return
        self.entered.set()
        assert self.release.wait(2)
        if self.fail_first:
            raise ValueError("first attempt failed")


@pytest.mark.parametrize("fail", [False, True])
def test_execution_finished_is_published_before_waiters_wake(fail: bool) -> None:
    """run 返回前 execution.finished 已发布，发布时 Execution 尚未写入终态。

    Args:
        fail: 是否让 Graph 执行失败。
    """

    class Finish(Node):
        """立即完成或失败的节点。"""

        def execute(self, inputs, context: Context) -> None:
            """按参数完成或抛出异常。

            Args:
                inputs: 未使用的入口数据。
                context: 未使用的执行上下文。

            Raises:
                ValueError: fail 为 True。
            """

            del inputs, context
            if fail:
                raise ValueError("graph failed")

    finished: list[tuple[RuntimeEvent, bool]] = []
    with Runtime() as runtime:
        runtime.register("finish", Graph(entrypoint="node").add("node", Finish()))

        def observe(event: RuntimeEvent) -> None:
            """记录终态事件及发布时 Execution 是否已经结束。

            Args:
                event: Worker 发布的生命周期事件。
            """

            if event.kind is RuntimeEventKind.EXECUTION_FINISHED:
                assert event.execution_id is not None
                execution = runtime.get_execution(event.execution_id)
                finished.append((event, execution.done))

        handle = runtime.observe_runtime(observe)
        if fail:
            with pytest.raises(Exception, match="graph failed"):
                runtime.run("finish")
        else:
            assert runtime.run("finish") == ()
        handle.detach()

    assert len(finished) == 1
    event, done_at_publish = finished[0]
    assert not done_at_publish
    assert event.status == ("failed" if fail else "succeeded")
    assert event.error_type == ("ExecutionError" if fail else None)


def test_redelivered_work_replaces_finished_record_and_rejects_active_duplicate() -> (
    None
):
    """同一 Work.id 在前次执行进行中被拒绝，结束后重投递替换保留记录。"""

    node = Gate(fail_first=True)
    tasks = memory.TaskBackend()
    worker = GraphWorker(consumer=tasks, close_injected=True)
    results: list[tuple[str | None, int]] = []
    rejected = ThreadEvent()

    def observe(event: RuntimeEvent) -> None:
        """记录每次交付的结论与尝试序号。

        Args:
            event: Worker 发布的生命周期事件。
        """

        if event.kind is RuntimeEventKind.WORK_FINISHED:
            results.append((event.status, event.attributes["attempt"]))
            if event.status == "rejected":
                rejected.set()

    worker.observe_runtime(observe)
    worker.register("gate", Graph(entrypoint="node").add("node", node))
    worker.consume("gate", concurrency=2)
    work = Work("gate")
    try:
        tasks.submit("gate", work)
        assert node.entered.wait(1)
        first = worker.get_execution(work.id)
        # 前次执行仍在进行，同一 Work 的并发交付被拒绝，不覆盖活跃记录。
        tasks.submit("gate", work)
        assert rejected.wait(1)
        assert results == [("rejected", 1)]
        assert worker.get_execution(work.id) is first
    finally:
        node.release.set()
    # 后端先传播首个失败，即重复交付的拒绝原因。
    with pytest.raises(InterlaceRuntimeError, match="still active"):
        worker.wait_idle(2)
    assert first.status is ExecutionStatus.FAILED

    tasks.submit("gate", work)
    worker.wait_idle(2)
    retried = worker.get_execution(work.id)
    assert retried is not first
    assert retried.status is ExecutionStatus.SUCCEEDED
    assert [item.id for item in worker.executions()] == [work.id]
    worker.close()


def test_history_limit_and_direct_workers_are_constructor_parameters() -> None:
    """Execution 记录上限与直接执行线程数通过构造参数配置并校验。"""

    tasks = memory.TaskBackend()
    try:
        for invalid in (0, -1):
            with pytest.raises(ValueError, match="history_limit"):
                GraphWorker(consumer=tasks, history_limit=invalid)
            with pytest.raises(ValueError, match="direct_workers"):
                GraphWorker(consumer=tasks, direct_workers=invalid)
        with pytest.raises(TypeError, match="history_limit"):
            GraphWorker(consumer=tasks, history_limit=True)
    finally:
        tasks.close()

    node = Gate()
    plugin = LocalRuntimePlugin(history_limit=1, direct_workers=1)
    with Runtime(plugins=(plugin,)) as runtime:
        runtime.register("gate", Graph(entrypoint="node").add("node", node))
        blocked = runtime.start("gate")
        assert node.entered.wait(1)
        # 单线程直接执行池中，第二次执行排在阻塞执行之后；未结束记录不被淘汰。
        queued = runtime.start("gate")
        assert queued.status is ExecutionStatus.PENDING
        assert runtime.executions() == (blocked, queued)
        node.release.set()
        runtime.wait_idle(2)
        assert runtime.executions() == (queued,)
        with pytest.raises(InterlaceRuntimeError, match="unknown execution"):
            runtime.get_execution(blocked.id)


def test_on_rolls_back_only_the_consumer_it_created_when_route_fails() -> None:
    """route 失败时 on 撤销本次新建的消费者，保留此前已有的消费者。"""

    class FailingRouter(EventRouter):
        """在测试放行前拒绝所有 route 的 Router。

        Attributes:
            failing: 为 True 时 route 抛出 RuntimeError。
        """

        failing = True

        def route(self, *args: Any, **kwargs: Any) -> Any:
            """按 failing 拒绝或委托给 EventRouter.route。

            Args:
                *args: 透传给 EventRouter.route 的位置参数。
                **kwargs: 透传给 EventRouter.route 的关键字参数。

            Returns:
                EventRouter.route 的返回值。

            Raises:
                RuntimeError: failing 为 True。
            """

            if self.failing:
                raise RuntimeError("route rejected")
            return super().route(*args, **kwargs)

    bus = memory.EventBus()
    tasks = memory.TaskBackend()
    router = FailingRouter(events=bus, publisher=tasks)
    worker = GraphWorker(consumer=tasks, emit=router.publish)
    try:
        with Runtime(router=router, worker=worker) as runtime:
            with pytest.raises(RuntimeError, match="route rejected"):
                runtime.on("value", graph="graph", queue="fresh")
            with pytest.raises(Exception, match="no active consumers"):
                tasks.submit("fresh", Work("graph"))

            runtime.consume("kept")
            with pytest.raises(RuntimeError, match="route rejected"):
                runtime.on("value", graph="graph", queue="kept")
            with pytest.raises(InterlaceRuntimeError, match="local concurrency 1"):
                runtime.consume("kept", concurrency=2)

            router.failing = False
            # 首次失败没有遗留 concurrency=1 的消费者，因此可以换用新配置。
            runtime.on("value", graph="graph", queue="fresh", concurrency=2)
    finally:
        tasks.close()
        bus.close()
