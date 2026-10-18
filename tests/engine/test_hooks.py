"""动态 Node Hook、控制信号和异步执行桥接的契约测试。"""

from __future__ import annotations

import asyncio
import logging
from threading import Event as ThreadEvent
from threading import get_ident

import pytest

from interlace import Execution, Graph, Node, Output, Ports, Runtime
from interlace.engine.errors import (
    HookExecutionError,
    InvalidOutputError,
    PortValueTypeError,
)
from interlace.engine.executor import Engine
from interlace.engine.hooks import (
    NodeCall,
    NodeHook,
    ShortCircuit,
    StopGraph,
)


class AddOne(Node):
    """当前契约测试使用的 AddOne 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        calls: 按顺序记录的测试调用信息。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(result=int)

    def __init__(self) -> None:
        """初始化实例及其依赖，建立当前对象独立维护的状态。"""

        self.calls = 0

    def execute(self, inputs, context) -> Output:
        """记录调用次数，并输出加一后的值。"""

        del context
        self.calls += 1
        return Output(inputs["value"] + 1, "result")


def single_node_graph(node: Node) -> Graph:
    """构造只有一个节点的 Hook 契约测试 Graph。"""

    return Graph(entrypoint="work").add("work", node)


def test_object_hook_transforms_inputs_and_outputs() -> None:
    """验证对象 Hook 可以转换节点输入和输出。"""

    node = AddOne()
    order: list[str] = []

    class Transform(NodeHook):
        def enter(self, call: NodeCall) -> NodeCall:
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            order.append("enter")
            return call.with_inputs({"value": call.inputs["value"] * 2})

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            order.append("exit")
            return Output(outputs.value + 3, "result")

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(Transform(), graph="work.graph", node="work")
        outputs = runtime.run("work.graph", 4)

    assert outputs == (Output(12, "result"),)
    assert node.calls == 1
    assert order == ["enter", "exit"]


def test_short_circuit_skips_node_and_continues_through_edges() -> None:
    """验证短路跳过节点并沿原边继续传递输出。"""

    request = AddOne()

    class Double(Node):
        """当前契约测试使用的 Double 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            """输出两倍的输入值。"""

            del context
            return Output(inputs["value"] * 2, "result")

    graph = (
        Graph(entrypoint="request")
        .add("request", request)
        .add("parse", Double())
        .connect(
            "request",
            "parse",
            source_port="result",
            target_port="value",
        )
    )

    class Cached(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            del call
            raise ShortCircuit(Output(10, "result"))

    with Runtime() as runtime:
        runtime.register("request.graph", graph)
        runtime.attach(Cached(), graph="request.graph", node="request")
        outputs = runtime.run("request.graph", 1)

    assert request.calls == 0
    assert outputs == (Output(20, "result"),)


def test_short_circuit_runs_exit_for_entered_hooks_in_reverse_order() -> None:
    """验证短路后按逆序执行已进入 Hook 的出口。"""

    node = AddOne()
    calls: list[str] = []

    class Outer(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            calls.append("outer.enter")
            return call

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            calls.append("outer.exit")
            return outputs

    class Inner(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            del call
            calls.append("inner.enter")
            raise ShortCircuit(Output(5, "result"))

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            calls.append("inner.exit")
            return outputs

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(Outer(), graph="work.graph")
        runtime.attach(Inner(), graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(5, "result"),)
    assert calls == [
        "outer.enter",
        "inner.enter",
        "inner.exit",
        "outer.exit",
    ]


def test_stop_graph_ends_without_running_remaining_nodes() -> None:
    """验证停止信号阻止剩余节点执行。"""

    first = AddOne()
    second = AddOne()
    graph = (
        Graph(entrypoint="first")
        .add("first", first)
        .add("second", second)
        .connect("first", "second", source_port="result", target_port="value")
    )

    class Stop(NodeHook):
        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。
            """

            del call, outputs
            raise StopGraph(Output(99, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        runtime.attach(Stop(), graph="work.graph", node="first")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(99, "result"),)
    assert first.calls == 1
    assert second.calls == 0


def test_error_hook_recovers_and_then_runs_outer_exit() -> None:
    """验证错误 Hook 恢复后继续执行外层出口 Hook。"""

    calls: list[str] = []

    class Broken(Node):
        """当前契约测试使用的 Broken 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            """抛出可被 Hook 恢复的业务异常。"""

            del inputs, context
            raise ValueError("temporary")

    class Outer(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            calls.append("outer.enter")
            return call

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            calls.append("outer.exit")
            return outputs

        def error(self, call, error):
            """在错误阶段执行回调，未恢复的异常继续传播。

            Args:
                call: Hook 当前处理的节点调用记录。
                error: 需要传播、记录或用于恢复的异常。
            """

            del call
            calls.append(f"outer.error:{error}")
            raise error

    class Recover(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            calls.append("recover.enter")
            return call

        def error(self, call, error):
            """在错误阶段执行回调，未恢复的异常继续传播。

            Args:
                call: Hook 当前处理的节点调用记录。
                error: 需要传播、记录或用于恢复的异常。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            calls.append(f"recover.error:{error}")
            return (Output(7, "result"),)

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            calls.append("recover.exit")
            return outputs

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Broken()))
        runtime.attach(Outer(), graph="work.graph")
        runtime.attach(Recover(), graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(7, "result"),)
    assert calls == [
        "outer.enter",
        "recover.enter",
        "recover.error:temporary",
        "recover.exit",
        "outer.exit",
    ]


def test_single_function_hooks_support_all_phases() -> None:
    """验证单函数 Hook 支持入口、出口和错误阶段。"""

    node = AddOne()

    def enter(call: NodeCall) -> NodeCall:
        """在节点执行前调用入口 Hook 并取得调用参数。

        Args:
            call: Hook 当前处理的节点调用记录。

        Returns:
            同步调用或异步等待完成后的处理结果。
        """

        return call.with_inputs({"value": call.inputs["value"] + 1})

    async def exit(call: NodeCall, outputs):
        """在节点完成后调用出口 Hook 并取得输出。

        Args:
            call: Hook 当前处理的节点调用记录。
            outputs: 按交付顺序组织的节点输出。

        Returns:
            同步调用或异步等待完成后的处理结果。
        """

        del call
        await asyncio.sleep(0)
        return Output(outputs.value * 2, "result")

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(enter, phase="enter", graph="work.graph")
        runtime.attach(exit, phase="exit", graph="work.graph")
        outputs = runtime.run("work.graph", 2)

    assert outputs == (Output(8, "result"),)


def test_async_node_and_async_hooks_share_background_loop() -> None:
    """验证异步节点与异步 Hook 使用同一后台循环。"""

    loop_threads: list[int] = []
    caller_thread = get_ident()

    class AsyncWork(Node):
        """当前契约测试使用的 AsyncWork 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        async def execute(self, inputs, context):
            """在 runner 循环线程上记录线程 ID，并输出加一后的值。"""

            del context
            await asyncio.sleep(0)
            loop_threads.append(get_ident())
            return Output(inputs["value"] + 1, "result")

    class AsyncHook(NodeHook):
        async def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            await asyncio.sleep(0)
            loop_threads.append(get_ident())
            return call

        async def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            await asyncio.sleep(0)
            loop_threads.append(get_ident())
            return outputs

    with Runtime() as runtime:
        runtime.register("async.graph", single_node_graph(AsyncWork()))
        runtime.attach(AsyncHook(), graph="async.graph")
        outputs = runtime.run("async.graph", 1)

    assert outputs == (Output(2, "result"),)
    assert len(set(loop_threads)) == 1
    assert loop_threads[0] != caller_thread


def test_hook_detach_is_hot_and_idempotent() -> None:
    """验证 Hook 可以动态且幂等地卸载。"""

    node = AddOne()

    def double(call, outputs):
        """将测试输入翻倍，验证函数 Hook 能转换输入。

        Args:
            call: Hook 当前处理的节点调用记录。
            outputs: 按交付顺序组织的节点输出。

        Returns:
            输入经过倍增处理的 NodeCall。
        """

        del call
        return Output(outputs.value * 2, "result")

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        handle = runtime.attach(double, phase="exit", graph="work.graph")
        assert runtime.run("work.graph", 1) == (Output(4, "result"),)
        handle.detach()
        handle.detach()
        assert runtime.run("work.graph", 1) == (Output(2, "result"),)


def test_execution_uses_hook_snapshot_until_graph_finishes() -> None:
    """验证 Graph 整次执行使用固定的 Hook 快照。"""

    entered = ThreadEvent()
    release = ThreadEvent()

    class First(AddOne):
        def execute(self, inputs, context):
            """通知已进入，等测试放行后再执行父类逻辑。"""

            entered.set()
            release.wait(1)
            return super().execute(inputs, context)

    graph = (
        Graph(entrypoint="first")
        .add("first", First())
        .add("second", AddOne())
        .connect("first", "second", source_port="result", target_port="value")
    )
    seen: list[str] = []

    def record(call):
        """记录当前回调观察到的数据供测试断言。

        Args:
            call: Hook 当前处理的节点调用记录。

        Returns:
            当前测试回调收集或构造的结果。
        """

        seen.append(call.node_id)
        return call

    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        from threading import Thread

        result: list[tuple[Output, ...]] = []
        thread = Thread(target=lambda: result.append(runtime.run("work.graph", 1)))
        thread.start()
        assert entered.wait(1)
        runtime.attach(record, graph="work.graph")
        release.set()
        thread.join(1)
        assert not thread.is_alive()

        assert seen == []
        assert result == [(Output(3, "result"),)]
        runtime.run("work.graph", 1)
        assert seen == ["first", "second"]


def test_hook_outputs_still_obey_node_contract() -> None:
    """验证 Hook 输出仍受节点端口契约约束。"""

    node = AddOne()

    class Invalid(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            del call
            raise ShortCircuit(Output("wrong", "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(Invalid(), graph="work.graph")
        with pytest.raises(Exception, match="expected int"):
            runtime.run("work.graph", 1)


def test_short_circuit_is_rejected_outside_enter() -> None:
    """验证入口以外阶段拒绝短路信号。"""

    class Invalid(NodeHook):
        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。
            """

            del call, outputs
            raise ShortCircuit(Output(1, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.attach(Invalid(), graph="work.graph")
        with pytest.raises(HookExecutionError, match="only valid during hook enter"):
            runtime.run("work.graph", 1)


def test_control_signal_is_rejected_when_raised_by_node() -> None:
    """验证节点自身不能伪造 Hook 流程控制信号。"""

    class Invalid(Node):
        """当前契约测试使用的 Invalid 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            """违规地在节点内抛出 StopGraph。"""

            del inputs, context
            raise StopGraph(Output(1, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Invalid()))
        with pytest.raises(HookExecutionError, match="only be raised by a hook"):
            runtime.run("work.graph", 1)


def test_async_hook_can_stop_graph() -> None:
    """验证异步 Hook 能终止 Graph。"""

    class Stop(NodeHook):
        async def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            del call
            await asyncio.sleep(0)
            raise StopGraph(Output(11, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.attach(Stop(), graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(11, "result"),)


@pytest.mark.parametrize(
    "stop_outputs", [(), (Output("stopped"),), (Output(1), Output(2))]
)
@pytest.mark.parametrize("mode", ["sync", "async", "engine"])
def test_stop_graph_preserves_published_outputs_in_result_and_stream(
    stop_outputs, mode
) -> None:
    """验证停止 Graph 后结果与输出流保留已发布输出。

    Args:
        stop_outputs: 当前用例使用的 stop_outputs 夹具或参数化输入。
        mode: 当前用例使用的 mode 夹具或参数化输入。
    """

    class Fan(Node):
        """当前契约测试使用的 Fan 替代实现。

        Attributes:
            output_ports: 节点声明的输出端口及其类型。
        """

        output_ports = Ports(default=object, next=object)

        def execute(self, inputs, context):
            """先输出默认端口，再把值发往 next 端口。"""

            return (Output("before"), Output("next", "next"))

    class End(Node):
        def execute(self, inputs, context):
            """图停止后不应被执行，执行即判定测试失败。"""

            raise AssertionError("stopped node must not execute")

    class Stop(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            raise StopGraph(*stop_outputs)

    graph = (
        Graph(entrypoint="first")
        .add(first=Fan(), last=End())
        .connect("first", "last", source_port="next")
    )
    expected = (Output("before"), *stop_outputs)
    if mode == "engine":
        from interlace import Execution
        from interlace.engine.executor import Engine

        engine = Engine()
        try:
            engine.attach(Stop(), graph="work", node="last")
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
        runtime.attach(Stop(), graph="work", node="last")
        if mode == "sync":
            assert tuple(runtime.iter("work", output_buffer=1)) == expected
        else:

            async def collect():
                """收集当前回调观察到的输出，供测试断言。

                Returns:
                    当前测试回调收集或构造的结果。
                """

                return tuple(
                    [output async for output in runtime.aiter("work", output_buffer=1)]
                )

            assert asyncio.run(collect()) == expected
        assert runtime.executions()[-1].result() == expected


def test_hook_scope_is_validated() -> None:
    """验证 Hook 作用域参数的合法性。"""

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        with pytest.raises(ValueError, match="requires graph"):
            runtime.attach(lambda call: call, node="work")
        with pytest.raises(ValueError, match="has no node"):
            runtime.attach(
                lambda call: call,
                graph="work.graph",
                node="missing",
            )


def test_invalid_function_hook_result_is_classified() -> None:
    """验证非法函数 Hook 返回值使用明确的错误类型。"""

    def invalid(call, outputs):
        """产生非法返回值以验证契约校验。

        Args:
            call: Hook 当前处理的节点调用记录。
            outputs: 按交付顺序组织的节点输出。

        Returns:
            故意不满足被测接口契约的返回值。
        """

        del call, outputs
        return "wrong"

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.attach(invalid, phase="exit", graph="work.graph")
        with pytest.raises(InvalidOutputError):
            runtime.run("work.graph", 1)


def test_async_enter_can_short_circuit() -> None:
    """验证异步入口 Hook 能短路节点。"""

    node = AddOne()

    class AsyncCache(NodeHook):
        async def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            del call
            await asyncio.sleep(0)
            raise ShortCircuit(Output(8, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(AsyncCache(), graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(8, "result"),)
    assert node.calls == 0


def test_function_error_hook_can_recover_async_node_error() -> None:
    """验证函数错误 Hook 能恢复异步节点的业务异常。"""

    class Broken(Node):
        """当前契约测试使用的 Broken 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        async def execute(self, inputs, context):
            """让出一次事件循环后抛出 LookupError。"""

            del inputs, context
            await asyncio.sleep(0)
            raise LookupError("missing")

    async def recover(call, error):
        """用测试指定的输出恢复业务异常。

        Args:
            call: Hook 当前处理的节点调用记录。
            error: 需要传播、记录或用于恢复的异常。

        Returns:
            用于恢复业务异常的测试输出。
        """

        del call
        await asyncio.sleep(0)
        assert isinstance(error, LookupError)
        return (Output(3, "result"),)

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Broken()))
        runtime.attach(recover, phase="error", graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(3, "result"),)


def run_on_engine(node: Node, *hooks: NodeHook) -> Execution:
    """在独立 Engine 上执行单节点 Graph，依次注册 hooks，并按结果结束 Execution。

    Returns:
        已进入终态的 Execution；失败时 error 保存 Engine 抛出的异常。
    """

    graph = single_node_graph(node).freeze()
    engine = Engine()
    execution = Execution("work")
    try:
        for hook in hooks:
            engine.attach(hook, graph="work")
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


@pytest.mark.parametrize(
    ("violation", "expected"),
    [
        ("node-output", InvalidOutputError),
        ("hook-call", HookExecutionError),
        ("hook-input-type", PortValueTypeError),
    ],
)
def test_engine_contract_errors_bypass_outer_error_hook(violation, expected) -> None:
    """Engine 契约错误像控制异常一样穿过外层 hook.error，不被当作业务异常恢复。

    Args:
        violation: node-output 为 Node 返回非 Output，hook-call 为内层 enter 返回非
            NodeCall，hook-input-type 为内层 enter 把输入替换为错误类型。
        expected: 调用方应看到的契约错误类型。
    """

    recovered: list[BaseException] = []

    class Invalid(Node):
        """node-output 场景返回字符串，其余场景正常加一。"""

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            """按场景返回非法值或合法 Output。"""

            del context
            if violation == "node-output":
                return "not an output"
            return Output(inputs["value"] + 1, "result")

    class Recover(NodeHook):
        """外层 Hook：记录交给 error 的异常并尝试恢复。"""

        def error(self, call, error):
            """记录异常并返回恢复输出。"""

            del call
            recovered.append(error)
            return Output(0, "result")

    class Break(NodeHook):
        """内层 Hook：按场景破坏 enter 契约。"""

        def enter(self, call):
            """返回非 NodeCall 或替换为错误类型的输入。"""

            if violation == "hook-call":
                return "not a call"
            if violation == "hook-input-type":
                return call.with_inputs({"value": "text"})
            return call

    execution = run_on_engine(Invalid(), Recover(), Break())

    assert isinstance(execution.error, expected)
    assert recovered == []


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
        output: Hook 随 StopGraph 交付的输出。
        expected: 违反端口或类型契约时的错误类型。
    """

    class Stop(NodeHook):
        """在入口阶段携带参数化输出停止 Graph。"""

        def enter(self, call):
            """抛出携带 output 的 StopGraph。"""

            raise StopGraph(output)

    execution = run_on_engine(AddOne(), Stop())

    assert isinstance(execution.error, expected)
    assert (execution.error.graph, execution.error.node) == ("work", "work")
    with pytest.raises(expected):
        next(iter(execution))


def test_failing_inspect_is_logged_once_per_execution_with_count(caplog) -> None:
    """持续失败的 inspect 只记录一次 traceback，结束时汇总失败次数。

    Args:
        caplog: 捕获 Engine 日志。
    """

    class Many(Node):
        """产出三项输出，使 inspect 在 ENTER 与每项 EXIT 各被调用一次。"""

        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            """返回三项输出。"""

            del context
            return tuple(
                Output(inputs["default"] + index, "result") for index in range(3)
            )

    class Broken(NodeHook):
        """每次 inspect 都抛错的诊断 Hook。"""

        def inspect(self, call, phase, value):
            """总是失败。

            Raises:
                RuntimeError: 预设的诊断失败。
            """

            raise RuntimeError("inspect failed")

    with caplog.at_level(logging.WARNING, logger="interlace.engine.executor"):
        execution = run_on_engine(Many(), Broken())

    assert execution.result() == (
        Output(1, "result"),
        Output(2, "result"),
        Output(3, "result"),
    )
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 1 and "inspect failed" in caplog.text
    assert "failed 4 times" in caplog.text
