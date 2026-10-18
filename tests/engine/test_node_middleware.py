"""NODE_CALL 中间件与 NODE_INSPECT 采集的契约测试。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from dataclasses import replace
from threading import Event as ThreadEvent
from threading import get_ident
from typing import Any

import pytest

from interlace import Context, Graph, Node, Output, Runtime
from interlace.engine import Execution
from interlace.engine.errors import (
    HookExecutionError,
    InvalidOutputError,
    PortValueTypeError,
)
from interlace.engine.executor import Engine
from interlace.plugins import HookBus
from interlace.plugins.points import (
    NODE_CALL,
    NODE_INSPECT,
    HookPhase,
    NodeCall,
    NodeInspection,
    StopGraph,
)


class AddOne(Node):
    """输出输入加一并记录调用次数的节点。

    Attributes:
        inputs: 节点声明的输入端口及其类型。
        outputs: 节点声明的输出端口及其类型。
        calls: 被调用的次数。
    """

    inputs = {"value": int}
    outputs = {"result": int}

    def __init__(self) -> None:
        """初始化调用计数。"""

        self.calls = 0

    def execute(self, inputs, context) -> Output:
        """记录调用次数，并输出加一后的值。"""

        del context
        self.calls += 1
        return Output(inputs["value"] + 1, "result")


def single_node_graph(node: Node) -> Graph:
    """构造只有一个节点 work 的 Graph。"""

    return Graph(entrypoint="work").add("work", node)


def run_on_engine(node: Node, *middleware: Any) -> Execution:
    """在独立 Engine 上执行单节点 Graph，依次登记中间件，并按结果结束 Execution。

    Returns:
        已进入终态的 Execution；失败时 error 保存 Engine 抛出的异常。
    """

    graph = single_node_graph(node).freeze()
    hooks = HookBus()
    for handler in middleware:
        hooks.on(NODE_CALL, handler)
    engine = Engine(hooks=hooks)
    execution = Execution("work")
    try:
        execution.start(graph)
        try:
            engine.execute("work", graph, 1, lambda event: None, execution=execution)
        except BaseException as exc:
            execution.fail(exc)
        else:
            execution.succeed()
    finally:
        engine.close()
    return execution


def test_middleware_transforms_inputs_and_outputs_within_scope() -> None:
    """作用域匹配的中间件改写输入与输出；其他节点不受影响。"""

    node = AddOne()

    def triple(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """把输入乘三，并把输出再乘三。"""

        for output in call_next(call.with_inputs({"value": call.inputs["value"] * 3})):
            yield Output(output.value * 3, output.port)

    graph = (
        Graph(entrypoint="work")
        .add(work=node, other=AddOne())
        .connect("work", "other", source_port="result", target_port="value")
    )
    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        runtime.hooks.on(NODE_CALL, triple, graph="work.graph", node="work")
        assert runtime.run("work.graph", 4) == (Output(40, "result"),)


def test_short_circuit_skips_node_and_continues_through_edges() -> None:
    """不调用 call_next 即跳过节点，返回的输出沿原有边继续传播。"""

    request = AddOne()
    parse = AddOne()
    graph = (
        Graph(entrypoint="request")
        .add(request=request, parse=parse)
        .connect("request", "parse", source_port="result", target_port="value")
    )
    with Runtime() as runtime:
        runtime.register("request.graph", graph)
        runtime.hooks.on(
            NODE_CALL,
            lambda call, call_next: Output(10, "result"),
            graph="request.graph",
            node="request",
        )
        assert runtime.run("request.graph", 1) == (Output(11, "result"),)
    assert request.calls == 0
    assert parse.calls == 1


def test_middleware_nests_outer_first_and_short_circuit_passes_outer_layers() -> None:
    """先登记者位于外层；内层短路的输出仍经过外层改写。"""

    order: list[str] = []
    node = AddOne()

    def outer(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """记录进出并加一百。"""

        order.append("outer-in")
        for output in call_next(call):
            order.append("outer-out")
            yield Output(output.value + 100, output.port)

    def inner(call: NodeCall, call_next: Any) -> Output:
        """短路返回固定输出。"""

        order.append("inner")
        return Output(5, "result")

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.hooks.on(NODE_CALL, outer, graph="work.graph")
        runtime.hooks.on(NODE_CALL, inner, graph="work.graph")
        assert runtime.run("work.graph", 1) == (Output(105, "result"),)
    assert order == ["outer-in", "inner", "outer-out"]
    assert node.calls == 0


def test_stop_graph_ends_without_running_remaining_nodes() -> None:
    """中间件抛出 StopGraph 时不再执行后续节点，并交付携带的输出。"""

    first, second = AddOne(), AddOne()
    graph = (
        Graph(entrypoint="first")
        .add(first=first, second=second)
        .connect("first", "second", source_port="result", target_port="value")
    )

    def stop(call: NodeCall, call_next: Any) -> Output:
        """执行节点后停止 Graph。"""

        tuple(call_next(call))
        raise StopGraph(Output(99, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        runtime.hooks.on(NODE_CALL, stop, graph="work.graph", node="first")
        assert runtime.run("work.graph", 1) == (Output(99, "result"),)
    assert (first.calls, second.calls) == (1, 0)


def test_middleware_recovers_business_errors_and_outer_sees_recovery() -> None:
    """中间件可以恢复内层业务异常，恢复输出继续经过外层。"""

    class Broken(Node):
        """总是抛出业务异常的异步节点。"""

        inputs = {"value": int}
        outputs = {"result": int}

        async def execute(self, inputs, context):
            """让出一次事件循环后抛出 LookupError。"""

            del inputs, context
            await asyncio.sleep(0)
            raise LookupError("missing")

    def outer(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """把输出加一。"""

        for output in call_next(call):
            yield Output(output.value + 1, output.port)

    def recover(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """以替代输出恢复 LookupError。"""

        try:
            yield from call_next(call)
        except LookupError:
            yield Output(6, "result")

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Broken()))
        runtime.hooks.on(NODE_CALL, outer, graph="work.graph")
        runtime.hooks.on(NODE_CALL, recover, graph="work.graph")
        assert runtime.run("work.graph", 1) == (Output(7, "result"),)


def test_call_next_can_retry_the_node() -> None:
    """多次调用 call_next 会重新执行节点，可实现重试。"""

    class Flaky(Node):
        """首次失败、之后成功的节点。

        Attributes:
            calls: 被调用的次数。
        """

        inputs = {"value": int}
        outputs = {"result": int}
        calls = 0

        def execute(self, inputs, context):
            """首次抛出业务异常。"""

            del context
            type(self).calls += 1
            if type(self).calls == 1:
                raise ConnectionError("flaky")
            return Output(inputs["value"], "result")

    def retry(call: NodeCall, call_next: Any) -> tuple[Output, ...]:
        """业务失败时重试一次。"""

        try:
            return tuple(call_next(call))
        except ConnectionError:
            return tuple(call_next(call))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Flaky()))
        runtime.hooks.on(NODE_CALL, retry, graph="work.graph")
        assert runtime.run("work.graph", 3) == (Output(3, "result"),)
    assert Flaky.calls == 2


def test_call_next_rejects_identity_and_port_changes() -> None:
    """传给 call_next 的调用只能替换输入值，且输入值仍须符合端口类型。"""

    def foreign_context(call: NodeCall, call_next: Any) -> Any:
        """替换 Context。"""

        return call_next(replace(call, context=Context(parent=call.context)))

    def extra_port(call: NodeCall, call_next: Any) -> Any:
        """增加未声明的输入端口。"""

        return call_next(call.with_inputs({"value": 1, "extra": 2}))

    def wrong_type(call: NodeCall, call_next: Any) -> Any:
        """把输入替换为错误类型。"""

        return call_next(call.with_inputs({"value": "text"}))

    assert isinstance(
        run_on_engine(AddOne(), foreign_context).error, HookExecutionError
    )
    assert isinstance(run_on_engine(AddOne(), extra_port).error, HookExecutionError)
    assert isinstance(run_on_engine(AddOne(), wrong_type).error, PortValueTypeError)


def test_async_node_and_waited_middleware_share_background_loop() -> None:
    """异步节点与中间件经 call.wait 等待的协程运行在同一后台事件循环。"""

    loop_threads: list[int] = []

    class AsyncWork(Node):
        """记录事件循环线程的异步节点。"""

        inputs = {"value": int}
        outputs = {"result": int}

        async def execute(self, inputs, context):
            """记录线程并输出加一。"""

            del context
            loop_threads.append(get_ident())
            return Output(inputs["value"] + 1, "result")

    async def lookup(value: int) -> int:
        """记录线程并返回翻倍结果。"""

        loop_threads.append(get_ident())
        return value * 2

    def middleware(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """经 call.wait 调用异步接口转换输出。"""

        for output in call_next(call):
            yield Output(call.wait(lookup(output.value)), output.port)

    with Runtime() as runtime:
        runtime.register("async.graph", single_node_graph(AsyncWork()))
        runtime.hooks.on(NODE_CALL, middleware, graph="async.graph")
        assert runtime.run("async.graph", 1) == (Output(4, "result"),)
    assert len(loop_threads) == 2 and len(set(loop_threads)) == 1
    assert loop_threads[0] != get_ident()


def test_async_middleware_is_rejected() -> None:
    """中间件必须同步；返回 awaitable 时以 HookExecutionError 失败。"""

    async def middleware(call: NodeCall, call_next: Any) -> Output:
        """错误地声明为协程。"""

        return Output(1, "result")

    assert isinstance(run_on_engine(AddOne(), middleware).error, HookExecutionError)


def test_registration_detach_is_hot_and_idempotent() -> None:
    """注销从下一次 execution 生效，重复注销无作用。"""

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        registration = runtime.hooks.on(
            NODE_CALL,
            lambda call, call_next: Output(0, "result"),
            graph="work.graph",
        )
        assert runtime.run("work.graph", 1) == (Output(0, "result"),)
        registration.detach()
        registration.detach()
        assert runtime.run("work.graph", 1) == (Output(2, "result"),)


def test_execution_uses_middleware_snapshot_until_graph_finishes() -> None:
    """execution 开始后登记的中间件不影响进行中的执行，只作用于之后的执行。"""

    entered = ThreadEvent()
    release = ThreadEvent()

    class Gate(Node):
        """第一个节点等待测试放行。"""

        inputs = {"value": int}
        outputs = {"result": int}

        def execute(self, inputs, context):
            """等待放行后输出原值。"""

            del context
            entered.set()
            assert release.wait(1)
            return Output(inputs["value"], "result")

    graph = (
        Graph(entrypoint="gate")
        .add(gate=Gate(), add=AddOne())
        .connect("gate", "add", source_port="result", target_port="value")
    )
    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        execution = runtime.start("work.graph", 1)
        assert entered.wait(1)
        runtime.hooks.on(
            NODE_CALL,
            lambda call, call_next: Output(-1, "result"),
            graph="work.graph",
            node="add",
        )
        release.set()
        assert execution.result(1) == (Output(2, "result"),)
        entered.clear()
        assert runtime.run("work.graph", 1) == (Output(-1, "result"),)


def test_middleware_outputs_obey_node_contract() -> None:
    """中间件产生的输出仍按节点的输出端口与类型校验。"""

    wrong_port = run_on_engine(AddOne(), lambda call, call_next: Output(1, "missing"))
    wrong_type = run_on_engine(AddOne(), lambda call, call_next: Output("x", "result"))
    not_output = run_on_engine(AddOne(), lambda call, call_next: "wrong")

    assert isinstance(wrong_port.error, InvalidOutputError)
    assert isinstance(wrong_type.error, PortValueTypeError)
    assert isinstance(not_output.error, InvalidOutputError)


def test_node_cannot_raise_stop_graph() -> None:
    """节点自身不能伪造只属于中间件的 StopGraph。"""

    class Invalid(Node):
        """直接抛出 StopGraph 的节点。"""

        inputs = {"value": int}
        outputs = {"result": int}

        def execute(self, inputs, context):
            """抛出 StopGraph。"""

            raise StopGraph(Output(1, "result"))

    execution = run_on_engine(Invalid())
    assert isinstance(execution.error, HookExecutionError)


@pytest.mark.parametrize(
    "stop_outputs", [(), (Output("stopped"),), (Output(1), Output(2))]
)
@pytest.mark.parametrize("mode", ["sync", "async", "engine"])
def test_stop_graph_preserves_published_outputs_in_result_and_stream(
    stop_outputs, mode
) -> None:
    """停止 Graph 后结果与输出流保留之前已发布的输出。

    Args:
        stop_outputs: StopGraph 携带的输出。
        mode: 通过 Runtime 同步流、异步流或直接 Engine 执行。
    """

    class Fan(Node):
        """先输出默认端口，再把值发往 next 端口。

        Attributes:
            outputs: 节点声明的输出端口及其类型。
        """

        outputs = {"default": object, "next": object}

        def execute(self, inputs, context):
            """返回两项输出。"""

            return (Output("before"), Output("next", "next"))

    class End(Node):
        """图停止后不应被执行的节点。"""

        def execute(self, inputs, context):
            """执行即判定测试失败。"""

            raise AssertionError("stopped node must not execute")

    def stop(call: NodeCall, call_next: Any) -> Output:
        """在节点执行前停止 Graph。"""

        raise StopGraph(*stop_outputs)

    graph = (
        Graph(entrypoint="first")
        .add(first=Fan(), last=End())
        .connect("first", "last", source_port="next")
    )
    expected = (Output("before"), *stop_outputs)
    if mode == "engine":
        hooks = HookBus()
        hooks.on(NODE_CALL, stop, graph="work", node="last")
        engine = Engine(hooks=hooks)
        try:
            execution = Execution("work")
            execution.start(graph.freeze())
            engine.execute("work", graph, None, lambda e: None, execution=execution)
            execution.succeed()
            assert execution.result() == tuple(execution) == expected
        finally:
            engine.close()
        return

    with Runtime() as runtime:
        runtime.register("work", graph)
        runtime.hooks.on(NODE_CALL, stop, graph="work", node="last")
        if mode == "sync":
            assert tuple(runtime.iter("work", output_buffer=1)) == expected
        else:

            async def collect() -> tuple[Output, ...]:
                """收集异步输出流。"""

                return tuple(
                    [output async for output in runtime.aiter("work", output_buffer=1)]
                )

            assert asyncio.run(collect()) == expected
        assert runtime.executions()[-1].result() == expected


def test_middleware_scope_is_validated() -> None:
    """登记未声明的作用域键会明确失败。"""

    with Runtime() as runtime, pytest.raises(ValueError, match="no scope keys"):
        runtime.hooks.on(NODE_CALL, lambda call, call_next: None, queue="work")


@pytest.mark.parametrize(
    ("violation", "expected"),
    [
        ("node-output", InvalidOutputError),
        ("call", HookExecutionError),
        ("input-type", PortValueTypeError),
    ],
)
def test_engine_contract_errors_cannot_be_recovered_by_outer_middleware(
    violation, expected
) -> None:
    """Engine 契约错误穿过外层恢复中间件，外层捕获后也不能交付替代输出。

    Args:
        violation: node-output 为 Node 返回非 Output，call 为内层传入非 NodeCall，
            input-type 为内层把输入替换为错误类型。
        expected: 调用方应看到的契约错误类型。
    """

    class Invalid(Node):
        """node-output 场景返回字符串，其余场景正常加一。"""

        inputs = {"value": int}
        outputs = {"result": int}

        def execute(self, inputs, context):
            """按场景返回非法值或合法 Output。"""

            del context
            if violation == "node-output":
                return "not an output"
            return Output(inputs["value"] + 1, "result")

    def recover(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """外层尝试恢复全部普通异常。"""

        try:
            yield from call_next(call)
        except Exception:
            yield Output(0, "result")

    def breaker(call: NodeCall, call_next: Any) -> Any:
        """内层按场景破坏调用契约。"""

        if violation == "call":
            return call_next("not a call")
        if violation == "input-type":
            return call_next(call.with_inputs({"value": "text"}))
        return call_next(call)

    execution = run_on_engine(Invalid(), recover, breaker)

    assert isinstance(execution.error, expected)


@pytest.mark.parametrize(
    ("output", "expected"),
    [
        (Output(1, "undeclared"), InvalidOutputError),
        (Output("text", "result"), PortValueTypeError),
    ],
)
def test_stop_graph_outputs_obey_stopping_node_contract(output, expected) -> None:
    """StopGraph 携带的输出按停止节点声明的端口与类型校验。

    Args:
        output: 中间件随 StopGraph 交付的输出。
        expected: 违反端口或类型契约时的错误类型。
    """

    def stop(call: NodeCall, call_next: Any) -> Output:
        """抛出携带 output 的 StopGraph。"""

        raise StopGraph(output)

    execution = run_on_engine(AddOne(), stop)

    assert isinstance(execution.error, expected)
    assert (execution.error.graph, execution.error.node) == ("work", "work")
    with pytest.raises(expected):
        next(iter(execution))


def test_inspect_sees_actual_inputs_and_raw_outputs() -> None:
    """NODE_INSPECT 看到全部中间件改写后的输入，以及改写前的原始输出。"""

    seen: list[tuple[HookPhase, Any]] = []

    def double(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """输入翻倍，输出加一百。"""

        for output in call_next(call.with_inputs({"value": call.inputs["value"] * 2})):
            yield Output(output.value + 100, output.port)

    def inspect(inspection: NodeInspection) -> None:
        """记录阶段与实际值。"""

        value = (
            dict(inspection.call.inputs)
            if inspection.phase is HookPhase.ENTER
            else inspection.value
        )
        seen.append((inspection.phase, value))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.hooks.on(NODE_INSPECT, inspect)
        runtime.hooks.on(NODE_CALL, double)
        assert runtime.run("work.graph", 3) == (Output(107, "result"),)
    assert seen == [
        (HookPhase.ENTER, {"value": 6}),
        (HookPhase.EXIT, Output(7, "result")),
    ]


def test_failing_inspect_is_logged_once_per_execution_with_count(caplog) -> None:
    """持续失败的采集处理函数只记录一次 traceback，结束时汇总失败次数且不影响结果。

    Args:
        caplog: 捕获 Engine 日志。
    """

    class Many(Node):
        """产出三项输出，使采集在 ENTER 与每项 EXIT 各被调用一次。"""

        outputs = {"result": int}

        def execute(self, inputs, context):
            """返回三项输出。"""

            del context
            return tuple(
                Output(inputs["default"] + index, "result") for index in range(3)
            )

    def broken(inspection: NodeInspection) -> None:
        """总是失败。

        Raises:
            RuntimeError: 预设的诊断失败。
        """

        raise RuntimeError("inspect failed")

    graph = single_node_graph(Many()).freeze()
    hooks = HookBus()
    hooks.on(NODE_INSPECT, broken)
    engine = Engine(hooks=hooks)
    execution = Execution("work")
    try:
        execution.start(graph)
        with caplog.at_level(logging.WARNING, logger="interlace.engine.executor"):
            engine.execute("work", graph, 1, lambda event: None, execution=execution)
        execution.succeed()
    finally:
        engine.close()

    assert execution.result() == (
        Output(1, "result"),
        Output(2, "result"),
        Output(3, "result"),
    )
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    warnings = [
        record for record in caplog.records if record.levelno == logging.WARNING
    ]
    assert len(errors) == 1 and str(errors[0].exc_info[1]) == "inspect failed"
    assert len(warnings) == 1 and 4 in warnings[0].args
