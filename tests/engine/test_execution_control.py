"""Execution 身份、步数、超时和协作式取消的契约测试。"""

from __future__ import annotations

import asyncio
import logging
import time
import traceback
from contextlib import suppress
from threading import Event as ThreadEvent
from threading import Lock, Thread
from threading import enumerate as live_threads

import pytest

from interlace import (
    Context,
    Execution,
    ExecutionLimits,
    ExecutionStatus,
    Graph,
    InputPolicy,
    Node,
    Output,
    Ports,
    Runtime,
    SlotPool,
)
from interlace.adapters import memory
from interlace.engine import runner as runner_module
from interlace.engine.errors import (
    InterlaceRuntimeError,
    ExecutionCancelledError,
    ExecutionError,
    ExecutionTimeoutError,
    HookExecutionError,
    IncompleteInputsError,
    NodeTimeoutError,
    PolicySelectionError,
    PortValueTypeError,
    QueueLimitExceededError,
    StepLimitExceededError,
)
from interlace.engine.executor import Engine
from interlace.engine.hooks import NodeHook, StopGraph
from interlace.engine.policies import PolicyRef, PolicyRegistry
from interlace.engine.runner import LocalRunner
from interlace.runtime import GraphWorker, LocalRuntimePlugin


@pytest.mark.parametrize("control", ["cancel", "timeout"])
def test_async_cleanup_finishes_before_execution_releases_slot(control) -> None:
    """验证异步清理完成后才释放执行链 Slot。

    Args:
        control: 当前用例使用的 control 夹具或参数化输入。
    """

    started, cleaning, release, finished, next_started = (
        ThreadEvent() for _ in range(5)
    )
    seen_slots = []

    class Slow(Node):
        async def execute(self, inputs, context):
            """记录 Slot 后长时间等待；被取消时等释放信号再完成清理。"""

            seen_slots.append(context.slot)
            started.set()
            try:
                await asyncio.sleep(60)
            finally:
                cleaning.set()
                while not release.is_set():
                    await asyncio.sleep(0.005)
                finished.set()

    class Next(Node):
        def execute(self, inputs, context):
            """确认前一执行已完成清理，并记录本次分到的 Slot。"""

            assert finished.is_set()
            seen_slots.append(context.slot)
            next_started.set()

    slots = SlotPool(1)
    runtime = Runtime()
    try:
        runtime.register("slow", Graph(entrypoint="node").add(node=Slow()))
        runtime.register("next", Graph(entrypoint="node").add(node=Next()))
        runtime.on(
            "slow",
            graph="slow",
            queue="work",
            concurrency=2,
            slots=slots,
            timeout=0.1 if control == "timeout" else None,
        )
        runtime.on("next", graph="next", queue="work", concurrency=2, slots=slots)
        runtime.emit("slow")
        assert started.wait(2)
        execution = runtime.executions()[0]
        if control == "cancel":
            execution.cancel()
        assert cleaning.wait(2)
        runtime.emit("next")
        assert not execution.wait(0.1)
        assert slots.available == 0
        assert not next_started.is_set()
        release.set()
        assert next_started.wait(2)
        error = (
            ExecutionCancelledError if control == "cancel" else ExecutionTimeoutError
        )
        with pytest.raises(error):
            runtime.wait_idle(2)
        assert seen_slots[0] is seen_slots[1]
        assert slots.available == 1
    finally:
        release.set()
        with suppress(ExecutionCancelledError, ExecutionTimeoutError):
            runtime.close()
        slots.close()


class Increment(Node):
    """当前契约测试使用的 Increment 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(value=int)

    def execute(self, inputs, context: Context) -> Output:
        """输出加一后的值。"""

        del context
        return Output(inputs["value"] + 1, "value")


def looping_graph() -> Graph:
    """构造具有回边的计步测试 Graph。

    Returns:
        包含递增节点与回边的 Graph。
    """

    return (
        Graph(entrypoint="increment")
        .add(increment=Increment())
        .connect("increment", "increment", source_port="value", target_port="value")
    )


def test_execution_limits_default_to_unlimited() -> None:
    """验证默认执行限制不限制步数和时长。"""

    limits = ExecutionLimits()

    assert limits.max_steps == 0
    assert limits.timeout is None
    assert limits.max_queued == 0


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"max_steps": -1}, ValueError),
        ({"max_steps": 1.5}, TypeError),
        ({"timeout": 0}, ValueError),
        ({"timeout": float("nan")}, ValueError),
        ({"max_queued": -1}, ValueError),
        ({"max_queued": True}, TypeError),
    ],
)
def test_execution_limits_reject_invalid_values(kwargs, error) -> None:
    """验证执行限制拒绝非法类型和值。

    Args:
        kwargs: 传给目标接口的关键字参数。
        error: 需要传播、记录或用于恢复的异常。
    """

    with pytest.raises(error):
        ExecutionLimits(**kwargs)


def burst_graph() -> Graph:
    """构造入口一次产出多个值、下游逐个消费的 Graph。

    Returns:
        source 按 count 产出 0..count-1 到 sink 的 Graph。
    """

    class Burst(Node):
        """单次 firing 产出多个值的入口节点。"""

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """按 count 依次产出 0..count-1。"""

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    class Sink(Node):
        """逐个消费输入、不产生输出的下游节点。"""

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context):
            """丢弃收到的值。"""

            del inputs, context

    graph = Graph(entrypoint="source").add(source=Burst(), sink=Sink())
    return graph.connect("source", "sink", source_port="value", target_port="value")


def test_max_queued_stops_single_firing_burst_that_max_steps_cannot_see() -> None:
    """一次 firing 的输出积压超过 max_queued 时以 QUEUE_LIMITED 结束并指出积压端口。"""

    runtime = Runtime()
    runtime.register("burst", burst_graph())

    with pytest.raises(QueueLimitExceededError, match=r"max_queued=3") as caught:
        runtime.run("burst", 10, max_queued=3, max_steps=100)

    assert caught.value.node == "source"
    assert "sink.value=3" in str(caught.value)
    execution = runtime.executions()[-1]
    assert execution.status is ExecutionStatus.QUEUE_LIMITED
    assert execution.steps == 1
    runtime.close()


def test_max_queued_counts_consumed_tokens_and_allows_exact_boundary() -> None:
    """消费后的 token 不再计入；积压恰好等于上限时执行成功。"""

    with Runtime() as runtime:
        runtime.register("burst", burst_graph())
        assert runtime.run("burst", 3, max_queued=3) == ()
        execution = runtime.executions()[-1]
        assert execution.status is ExecutionStatus.SUCCEEDED
        assert execution.steps == 4


def test_routed_work_applies_max_queued_per_execution() -> None:
    """route 声明的 max_queued 作用于每个事件触发的 Work execution。"""

    with Runtime() as runtime:
        runtime.register("burst", burst_graph())
        runtime.on("burst.requested", graph="burst", queue="bursts", max_queued=2)
        runtime.emit("burst.requested", 5)
        with pytest.raises(QueueLimitExceededError):
            runtime.wait_idle(2)
        assert runtime.executions()[-1].status is ExecutionStatus.QUEUE_LIMITED


def test_max_steps_counts_node_firings_and_preserves_execution_state() -> None:
    """验证步数按节点触发计数且失败后保留执行状态。"""

    runtime = Runtime()
    runtime.register("loop.graph", looping_graph())

    with pytest.raises(StepLimitExceededError, match="max_steps=3"):
        runtime.run("loop.graph", 0, max_steps=3)

    execution = runtime.executions()[-1]
    assert execution.steps == 3
    assert execution.status is ExecutionStatus.STEP_LIMITED
    assert isinstance(execution.error, StepLimitExceededError)
    assert runtime.get_execution(execution.id) is execution
    runtime.close()


def test_max_steps_allows_exact_boundary() -> None:
    """验证恰好达到步数上限的执行能够成功。"""

    with Runtime() as runtime:
        runtime.register(
            "increment.graph",
            Graph(entrypoint="increment").add(increment=Increment()),
        )
        assert runtime.run("increment.graph", 1, max_steps=1) == (Output(2, "value"),)
        assert runtime.executions()[-1].steps == 1


def test_zero_max_steps_keeps_existing_unlimited_behavior() -> None:
    """验证零步数上限表示无限制。"""

    class StopAt(Node):
        """当前契约测试使用的 StopAt 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(again=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            """值未超过 20 时加一后回到 again 端口，否则输出到 done。"""

            del context
            value = inputs["value"]
            return Output(value + 1, "again" if value < 20 else "done")

    graph = (
        Graph(entrypoint="counter")
        .add(counter=StopAt())
        .connect("counter", "counter", source_port="again", target_port="value")
    )
    with Runtime() as runtime:
        runtime.register("counter.graph", graph)
        assert runtime.run("counter.graph", 0, max_steps=0) == (Output(21, "done"),)
        assert runtime.executions()[-1].steps == 21


def test_start_returns_successful_queryable_execution() -> None:
    """验证启动接口返回可查询的成功执行句柄。"""

    with Runtime() as runtime:
        runtime.register(
            "increment.graph",
            Graph(entrypoint="increment").add(increment=Increment()),
        )
        execution = runtime.start("increment.graph", 2)

        assert execution.result(1) == (Output(3, "value"),)
        assert execution.status is ExecutionStatus.SUCCEEDED
        assert execution.started_at is not None
        assert execution.finished_at is not None
        assert execution.steps == 1
        assert runtime.get_execution(execution.id) is execution


def test_execution_is_awaitable() -> None:
    """验证可以直接 await Execution 取得结果。"""

    async def scenario() -> tuple[Output, ...]:
        """组织当前测试的异步调用顺序与结果断言。

        Returns:
            符合声明端口契约的 Output 集合。
        """

        with Runtime() as runtime:
            runtime.register(
                "increment.graph",
                Graph(entrypoint="increment").add(increment=Increment()),
            )
            return await runtime.start("increment.graph", 2)

    assert asyncio.run(scenario()) == (Output(3, "value"),)


def test_runtime_iter_streams_terminal_outputs_and_keeps_final_result() -> None:
    """验证同步输出流与最终完整结果共享输出记录。"""

    class Many(Node):
        """当前契约测试使用的 Many 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """按 count 依次产生 0..count-1 个 Output。"""

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    with Runtime() as runtime:
        runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
        execution = runtime.start("many.graph", 3)

        assert tuple(execution) == (
            Output(0, "value"),
            Output(1, "value"),
            Output(2, "value"),
        )
        assert execution.result() == tuple(execution)
        assert tuple(runtime.iter("many.graph", 2)) == (
            Output(0, "value"),
            Output(1, "value"),
        )


def test_async_output_iteration() -> None:
    """验证异步迭代按产生顺序交付输出。"""

    class Many(Node):
        """当前契约测试使用的 Many 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """按 count 依次产生 0..count-1 个 Output。"""

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    async def scenario() -> list[Output]:
        """组织当前测试的异步调用顺序与结果断言。

        Returns:
            符合声明端口契约的 Output。
        """

        with Runtime() as runtime:
            runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
            return [output async for output in runtime.aiter("many.graph", 3)]

    assert asyncio.run(scenario()) == [
        Output(0, "value"),
        Output(1, "value"),
        Output(2, "value"),
    ]


def test_stream_yields_committed_outputs_before_later_failure() -> None:
    """验证后续失败不撤回已经交付的输出。"""

    class EmitThenFail(Node):
        """当前契约测试使用的 EmitThenFail 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int, next=int)

        def execute(self, inputs, context):
            """先输出终端结果，再把同一值交给失败节点。"""

            del context
            return Output(inputs["value"], "result"), Output(inputs["value"], "next")

    class Fail(Node):
        """当前契约测试使用的 Fail 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context):
            """在上游输出已交付后抛出业务异常。"""

            del inputs, context
            raise ValueError("after output")

    graph = (
        Graph(entrypoint="source")
        .add(source=EmitThenFail(), fail=Fail())
        .connect("source", "fail", source_port="next", target_port="value")
    )
    runtime = Runtime()
    runtime.register("partial.graph", graph)
    stream = runtime.iter("partial.graph", 7)

    assert next(stream) == Output(7, "result")
    with pytest.raises(ExecutionError, match="after output"):
        next(stream)
    runtime.close()


def test_active_stream_applies_bounded_backpressure() -> None:
    """验证活跃输出订阅对生产者施加有界背压。"""

    class Many(Node):
        """当前契约测试使用的 Many 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """按 count 依次产生 0..count-1 个 Output。"""

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    with Runtime() as runtime:
        runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
        # iter 在提交前订阅，避免执行在订阅前就已结束。
        stream = runtime.iter("many.graph", 3, output_buffer=1)
        execution = runtime.executions()[-1]

        assert next(stream) == Output(0, "value")
        time.sleep(0.05)
        assert not execution.done
        assert next(stream) == Output(1, "value")
        assert next(stream) == Output(2, "value")
        with pytest.raises(StopIteration):
            next(stream)
        assert execution.done


def test_unconsumed_stream_can_be_closed_without_blocking_execution() -> None:
    """验证关闭未消费输出流后执行不再受其阻塞。"""

    class Many(Node):
        """当前契约测试使用的 Many 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """按 count 依次产生 0..count-1 个 Output。"""

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    runtime = Runtime()
    runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
    stream = runtime.iter("many.graph", 3, output_buffer=1)
    close = stream.close
    assert callable(close)
    close()
    runtime.wait_idle(1)
    runtime.close()


@pytest.mark.parametrize("target", ["runtime", "worker"])
def test_close_releases_backpressure_of_unread_stream(target: str) -> None:
    """关闭不因未读订阅永久阻塞；执行排空后订阅仍能按序读到全部输出。

    Args:
        target: 被关闭的对象，Runtime 门面或单独的 GraphWorker。
    """

    class Many(Node):
        """按 count 一次产生多项输出的节点。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """按 count 依次产生 0..count-1 个 Output。"""

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    owner: Runtime | GraphWorker
    if target == "runtime":
        owner = Runtime()
    else:
        owner = GraphWorker(consumer=memory.TaskBackend(), close_injected=True)
    owner.register("many.graph", Graph(entrypoint="many").add(many=Many()))
    stream = owner.iter("many.graph", 5, output_buffer=1)
    execution = owner.executions()[-1]
    failures: list[BaseException] = []

    def close() -> None:
        """关闭目标并记录失败。"""

        try:
            owner.close()
        except BaseException as exc:
            failures.append(exc)

    closer = Thread(target=close, daemon=True)
    closer.start()
    closer.join(5)
    assert not closer.is_alive()
    assert failures == []
    assert execution.status is ExecutionStatus.SUCCEEDED
    assert list(stream) == [Output(value, "value") for value in range(5)]


def test_async_node_timeout_interrupts_awaitable() -> None:
    """验证异步节点超时会中断其等待对象。"""

    class Slow(Node):
        """当前契约测试使用的 Slow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.02

        async def execute(self, inputs, context: Context) -> Output:
            """异步休眠 1 秒后原样输出，用于触发超时。"""

            del context
            await asyncio.sleep(1)
            return Output(inputs["value"], "value")

    runtime = Runtime()
    runtime.register("slow.graph", Graph(entrypoint="slow").add(slow=Slow()))

    started = time.monotonic()
    with pytest.raises(NodeTimeoutError, match=r"timeout=0.02"):
        runtime.run("slow.graph", 1)
    assert time.monotonic() - started < 0.5
    assert runtime.executions()[-1].status is ExecutionStatus.TIMED_OUT
    runtime.close()


def test_none_timeouts_leave_slow_node_unlimited() -> None:
    """验证空超时不会终止较慢节点。"""

    class Slow(Node):
        """当前契约测试使用的 Slow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """异步休眠 30 毫秒后原样输出。"""

            del context
            await asyncio.sleep(0.03)
            return Output(inputs["value"], "value")

    with Runtime() as runtime:
        runtime.register("slow.graph", Graph(entrypoint="slow").add(slow=Slow()))
        assert runtime.run(
            "slow.graph",
            1,
            timeout=None,
        ) == (Output(1, "value"),)


def test_async_node_business_timeout_is_not_misclassified_as_control_timeout() -> None:
    """验证业务 TimeoutError 不会被误判为引擎控制超时。"""

    class Failing(Node):
        """当前契约测试使用的 Failing 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
            """抛出业务自身的 TimeoutError，而非执行控制超时。"""

            del inputs, context
            raise TimeoutError("upstream timed out")

    runtime = Runtime()
    runtime.register(
        "failing.graph",
        Graph(entrypoint="failing").add(failing=Failing()),
    )
    with pytest.raises(ExecutionError, match="upstream timed out") as raised:
        runtime.run("failing.graph", 1)
    assert not isinstance(raised.value, ExecutionTimeoutError)
    assert not isinstance(raised.value, NodeTimeoutError)
    runtime.close()


def test_graph_timeout_is_distinct_from_node_timeout() -> None:
    """验证 Graph 超时与节点超时采用不同异常。"""

    class SlowRelay(Node):
        """当前契约测试使用的 SlowRelay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.2

        async def execute(self, inputs, context: Context) -> Output:
            """异步休眠 20 毫秒后输出加一的值。"""

            del context
            await asyncio.sleep(0.02)
            return Output(inputs["value"] + 1, "value")

    graph = (
        Graph(entrypoint="first")
        .add(first=SlowRelay(), second=SlowRelay())
        .connect("first", "second", source_port="value", target_port="value")
    )
    runtime = Runtime()
    runtime.register("relay.graph", graph)
    with pytest.raises(ExecutionTimeoutError, match=r"timeout=0.03"):
        runtime.run(
            "relay.graph",
            0,
            timeout=0.03,
        )
    runtime.close()


def test_each_node_uses_its_own_timeout() -> None:
    """验证各节点分别使用自身的超时限制。"""

    class SlowRelay(Node):
        """当前契约测试使用的 SlowRelay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """异步休眠 20 毫秒后输出加一的值。"""

            del context
            await asyncio.sleep(0.02)
            return Output(inputs["value"] + 1, "value")

    first = SlowRelay()
    first.timeout = 0.1
    second = SlowRelay()
    second.timeout = 0.01
    graph = (
        Graph(entrypoint="first")
        .add(first=first, second=second)
        .connect("first", "second", source_port="value", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("per-node.graph", graph)
        with pytest.raises(NodeTimeoutError) as raised:
            runtime.run("per-node.graph", 0)

    assert raised.value.node == "second"


def test_graph_freeze_snapshots_node_timeout() -> None:
    """验证 Graph 冻结后使用节点超时快照。"""

    class Slow(Node):
        """当前契约测试使用的 Slow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """异步休眠 30 毫秒后原样输出。"""

            del context
            await asyncio.sleep(0.03)
            return Output(inputs["value"], "value")

    slow = Slow()
    slow.timeout = 0.01
    runtime = Runtime()
    runtime.register("snapshot.graph", Graph(entrypoint="slow").add(slow=slow))
    slow.timeout = None

    with pytest.raises(NodeTimeoutError):
        runtime.run("snapshot.graph", 1)
    runtime.close()


def test_sync_node_can_observe_timeout_at_context_checkpoint() -> None:
    """验证同步节点可在上下文检查点观察超时。"""

    class Cooperative(Node):
        """当前契约测试使用的 Cooperative 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.02

        def execute(self, inputs, context: Context) -> Output:
            """循环同步休眠并调用 checkpoint，直到被取消或超时。"""

            while True:
                time.sleep(0.005)
                context.checkpoint()

    runtime = Runtime()
    runtime.register(
        "cooperative.graph",
        Graph(entrypoint="cooperative").add(cooperative=Cooperative()),
    )
    with pytest.raises(NodeTimeoutError):
        runtime.run("cooperative.graph", 1)
    runtime.close()


def test_non_cooperative_sync_node_is_checked_when_it_returns() -> None:
    """验证不协作的同步节点返回后仍检查超时。"""

    class Blocking(Node):
        """当前契约测试使用的 Blocking 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.01

        def execute(self, inputs, context: Context) -> Output:
            """同步阻塞 30 毫秒后原样输出。"""

            del context
            time.sleep(0.03)
            return Output(inputs["value"], "value")

    runtime = Runtime()
    runtime.register(
        "blocking.graph",
        Graph(entrypoint="blocking").add(blocking=Blocking()),
    )
    with pytest.raises(NodeTimeoutError):
        runtime.run("blocking.graph", 1)
    runtime.close()


def test_execution_cancel_interrupts_async_node() -> None:
    """验证执行取消能够中断异步节点。"""

    class Waiting(Node):
        """当前契约测试使用的 Waiting 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """异步等待 10 秒，用于在等待中被取消。"""

            del context
            await asyncio.sleep(10)
            return Output(inputs["value"], "value")

    runtime = Runtime()
    runtime.register(
        "waiting.graph",
        Graph(entrypoint="waiting").add(waiting=Waiting()),
    )
    execution = runtime.start("waiting.graph", 1)
    deadline = time.monotonic() + 1
    while execution.status is ExecutionStatus.PENDING:
        assert time.monotonic() < deadline
        time.sleep(0.001)

    assert execution.cancel()
    with pytest.raises(ExecutionCancelledError):
        execution.result(1)
    assert execution.status is ExecutionStatus.CANCELLED
    assert execution.cancel_requested
    assert not execution.cancel()
    runtime.close()


def test_control_timeout_bypasses_hook_business_error_recovery() -> None:
    """验证控制超时不能被业务错误 Hook 恢复。"""

    class Waiting(Node):
        """当前契约测试使用的 Waiting 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.02

        async def execute(self, inputs, context: Context) -> Output:
            """异步等待 1 秒后原样输出。"""

            del context
            await asyncio.sleep(1)
            return Output(inputs["value"], "value")

    class Recover(NodeHook):
        def error(self, call, error):
            """在错误阶段执行回调，未恢复的异常继续传播。

            Args:
                call: Hook 当前处理的节点调用记录。
                error: 需要传播、记录或用于恢复的异常。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call, error
            return (Output(99, "value"),)

    runtime = Runtime()
    runtime.register(
        "waiting.graph",
        Graph(entrypoint="waiting").add(waiting=Waiting()),
    )
    runtime.attach(Recover(), graph="waiting.graph")
    with pytest.raises(NodeTimeoutError):
        runtime.run("waiting.graph", 1)
    runtime.close()


def test_arun_cancellation_cancels_underlying_execution() -> None:
    """验证取消 arun 等待方会取消底层执行。"""

    class Waiting(Node):
        """当前契约测试使用的 Waiting 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """异步等待 10 秒，用于在等待中被取消。"""

            del context
            await asyncio.sleep(10)
            return Output(inputs["value"], "value")

    async def scenario() -> ExecutionStatus:
        """组织当前测试的异步调用顺序与结果断言。

        Returns:
            观测到的执行状态。
        """

        runtime = Runtime()
        runtime.register(
            "waiting.graph",
            Graph(entrypoint="waiting").add(waiting=Waiting()),
        )
        task = asyncio.create_task(runtime.arun("waiting.graph", 1))
        while not runtime.executions():
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        execution = runtime.executions()[-1]
        assert execution.wait(1)
        runtime.close()
        return execution.status

    assert asyncio.run(scenario()) is ExecutionStatus.CANCELLED


def test_queue_work_inherits_route_execution_limits() -> None:
    """验证排队工作使用路由配置的执行限制。"""

    runtime = Runtime()
    runtime.register("loop.graph", looping_graph())
    runtime.on(
        "loop.requested",
        graph="loop.graph",
        queue="loops",
        max_steps=4,
    )
    runtime.emit("loop.requested", 0)

    with pytest.raises(StepLimitExceededError):
        runtime.wait_idle()
    execution = runtime.executions()[-1]
    assert execution.steps == 4
    assert execution.status is ExecutionStatus.STEP_LIMITED
    runtime.close()


def test_wait_idle_tracks_synchronous_run_from_another_thread() -> None:
    """验证空闲等待包含其他线程提交的同步执行。"""

    entered = ThreadEvent()
    release = ThreadEvent()

    class Blocking(Node):
        """当前契约测试使用的 Blocking 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            """通知已进入后阻塞，直到测试放行或 1 秒到期。"""

            del context
            entered.set()
            release.wait(1)
            return Output(inputs["value"], "value")

    runtime = Runtime()
    runtime.register(
        "blocking.graph",
        Graph(entrypoint="blocking").add(blocking=Blocking()),
    )
    thread = Thread(target=runtime.run, args=("blocking.graph", 1))
    thread.start()
    assert entered.wait(1)
    try:
        with pytest.raises(TimeoutError):
            runtime.wait_idle(0)
    finally:
        release.set()
        thread.join(1)
    assert not thread.is_alive()
    runtime.wait_idle(0)
    runtime.close()


def test_completed_execution_burst_is_trimmed_to_history_limit() -> None:
    """验证集中完成的执行记录按历史上限清理。"""

    entered = 0
    entered_all = ThreadEvent()
    counter_lock = Lock()
    release = ThreadEvent()

    class Block(Node):
        """当前契约测试使用的 Block 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """计数进入的并发执行，第三个到达时发信号，然后阻塞到放行。"""

            nonlocal entered
            del inputs, context
            with counter_lock:
                entered += 1
                if entered == 3:
                    entered_all.set()
            release.wait()

    with Runtime(plugins=(LocalRuntimePlugin(history_limit=2),)) as runtime:
        runtime.register("block.graph", Graph(entrypoint="block").add(block=Block()))
        executions = [runtime.start("block.graph", value) for value in range(3)]

        try:
            assert entered_all.wait(1)
            assert len(runtime.executions()) == 3
        finally:
            release.set()
        runtime.wait_idle(1)

        assert all(execution.done for execution in executions)
        assert len(runtime.executions()) == 2


def test_queued_unknown_graph_is_recorded_as_failed_execution() -> None:
    """验证未知 Graph 的排队工作被记录为失败执行。"""

    runtime = Runtime()
    runtime.consume("missing")
    runtime.route("missing.requested", graph="missing.graph", queue="missing")
    runtime.emit("missing.requested", 1)

    with pytest.raises(InterlaceRuntimeError, match="unknown registered graph"):
        runtime.wait_idle()
    execution = runtime.executions()[-1]
    assert execution.graph == "missing.graph"
    assert execution.status is ExecutionStatus.FAILED
    assert isinstance(execution.error, InterlaceRuntimeError)
    runtime.close()


def execute_directly(graph: Graph, inputs: object = None) -> Execution:
    """在独立 Engine 上执行已冻结或可默认冻结的 Graph，并按结果结束 Execution。

    Returns:
        已进入终态的 Execution；失败时 error 保存 Engine 抛出的异常。
    """

    if not graph.frozen:
        graph.freeze()
    engine = Engine()
    execution = Execution("work")
    try:
        execution.start(graph)
        try:
            engine.execute(
                "work", graph, inputs, lambda event: None, execution=execution
            )
        except BaseException as exc:
            execution.fail(exc)
        else:
            execution.succeed()
    finally:
        engine.close()
    return execution


class Burst(Node):
    """启动时向 value 端口产出 0、1、2。"""

    input_ports = Ports()
    input_policy = InputPolicy.ON_START
    output_ports = Ports(value=int)

    def execute(self, inputs, context: Context):
        """产出三项输出。"""

        del inputs, context
        return tuple(Output(index, "value") for index in range(3))


class Collect(Node):
    """使用测试 selector 消费 value，并把值原样作为终端输出。

    Attributes:
        seen: 按触发顺序记录的输入值。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(value=int)
    input_policy = PolicyRef("example.test/selector")

    def __init__(self) -> None:
        """创建空的触发记录。"""

        self.seen: list[int] = []

    def execute(self, inputs, context: Context) -> Output:
        """记录并转发输入值。"""

        del context
        self.seen.append(inputs["value"])
        return Output(inputs["value"], "value")


def selector_graph(selector: object, sink: Collect) -> Graph:
    """构造 Burst -> sink 的 Graph，并用给定 selector 冻结。"""

    policies = PolicyRegistry()
    policies.register("example.test/selector", selector)
    return (
        Graph(entrypoint="source")
        .add(source=Burst(), sink=sink)
        .connect("source", "sink", source_port="value", target_port="value")
        .freeze(policies)
    )


def test_selection_is_computed_once_per_firing() -> None:
    """入队时缓存的选择在出队时直接消费，每次 firing 只调用一次 selector。"""

    class Counting:
        """端口非空时选择全部端口，并记录调用次数。

        Attributes:
            calls: select 被调用的次数。
        """

        def __init__(self) -> None:
            """从零开始计数。"""

            self.calls = 0

        def select(self, ports, available, config):
            """计数后在全部端口非空时选择全部端口。"""

            del config
            self.calls += 1
            return tuple(ports) if all(available[port] for port in ports) else None

    selector, sink = Counting(), Collect()
    execution = execute_directly(selector_graph(selector, sink))

    assert execution.result() == (
        Output(0, "value"),
        Output(1, "value"),
        Output(2, "value"),
    )
    assert sink.seen == [0, 1, 2]
    # 首次就绪判定一次，之后每次 firing 结束重新判定一次（最后一次返回 None）。
    assert selector.calls == len(sink.seen) + 1


@pytest.mark.parametrize("failure", ["raises", "invalid"])
def test_selector_failures_are_attributed_to_graph_and_node(failure) -> None:
    """selector 抛错或违反契约都表现为带 Graph 与节点归属的 PolicySelectionError。

    Args:
        failure: raises 表示 selector 抛出普通异常，invalid 表示返回未声明端口。
    """

    cause = ValueError("selector broke")

    class Broken:
        """按参数抛错或选择未声明端口的 selector。"""

        def select(self, ports, available, config):
            """按场景失败。

            Raises:
                ValueError: raises 场景的预设异常。
            """

            del ports, available, config
            if failure == "raises":
                raise cause
            return ("missing",)

    sink = Collect()
    execution = execute_directly(selector_graph(Broken(), sink))

    error = execution.error
    assert isinstance(error, PolicySelectionError)
    assert (error.graph, error.node) == ("work", "sink")
    assert error.__cause__ is (cause if failure == "raises" else None)
    assert execution.status is ExecutionStatus.FAILED
    assert sink.seen == []


class Register(Node):
    """启动时通过 Context 注册测试提供的静止回调。

    Attributes:
        register: 接收本次 Context 并注册回调的函数。
    """

    input_ports = Ports()
    input_policy = InputPolicy.ON_START
    output_ports = Ports()

    def __init__(self, register) -> None:
        """保存注册函数。"""

        self.register = register

    def execute(self, inputs, context: Context) -> None:
        """把 Context 交给注册函数。"""

        del inputs
        self.register(context)


def test_quiescence_runs_callbacks_registered_by_callbacks_in_order() -> None:
    """静止回调按注册顺序运行，回调期间新注册的回调排在末尾并同样运行。"""

    calls: list[str] = []

    def register(context: Context) -> None:
        """注册一个会继续注册回调的回调。"""

        def first() -> None:
            """记录并追加嵌套回调。"""

            calls.append("first")
            context.on_quiescence(lambda: calls.append("nested"))

        context.on_quiescence(first)
        context.on_quiescence(lambda: calls.append("second"))

    execution = execute_directly(Graph(entrypoint="node").add(node=Register(register)))

    assert execution.status is ExecutionStatus.SUCCEEDED
    assert calls == ["first", "second", "nested"]


@pytest.mark.parametrize(
    "error", [ValueError("plain"), IncompleteInputsError("unmatched")]
)
def test_quiescence_failure_is_execution_error_with_graph(error) -> None:
    """普通异常包装为 ExecutionError，ExecutionError 子类补齐 graph；后续回调不运行。

    Args:
        error: 第一个回调抛出的异常。
    """

    calls: list[str] = []

    def register(context: Context) -> None:
        """注册一个失败回调和一个不应运行的回调。"""

        def fail() -> None:
            """抛出参数化异常。

            Raises:
                Exception: 参数化的回调异常。
            """

            raise error

        context.on_quiescence(fail)
        context.on_quiescence(lambda: calls.append("after"))

    execution = execute_directly(Graph(entrypoint="node").add(node=Register(register)))

    failure = execution.error
    assert isinstance(failure, ExecutionError)
    assert failure.graph == "work"
    if isinstance(error, ExecutionError):
        assert failure is error
    else:
        assert failure.__cause__ is error
    assert calls == []


def test_quiescence_does_not_run_after_incomplete_inputs() -> None:
    """残留输入使 execution 失败时不调用静止回调。"""

    calls: list[str] = []

    class Pair(Node):
        """需要 left 与 right 同时就绪的节点。"""

        input_ports = Ports(left=int, right=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """不应被调用。"""

            raise AssertionError("pair must not fire")

    class Source(Node):
        """只向 left 产出一项、right 不产出，并注册静止回调。"""

        input_ports = Ports()
        input_policy = InputPolicy.ON_START
        output_ports = Ports(left=int, right=int)

        def execute(self, inputs, context: Context) -> Output:
            """注册回调并产出半组输入。"""

            del inputs
            context.on_quiescence(lambda: calls.append("quiescence"))
            return Output(1, "left")

    graph = (
        Graph(entrypoint="source")
        .add(source=Source(), pair=Pair())
        .connect("source", "pair", source_port="left", target_port="left")
        .connect("source", "pair", source_port="right", target_port="right")
    )
    execution = execute_directly(graph)

    assert isinstance(execution.error, IncompleteInputsError)
    assert calls == []


def started_execution(limits: ExecutionLimits | None = None) -> Execution:
    """创建已绑定单节点 Graph 并进入 RUNNING 的 Execution。"""

    graph = Graph(entrypoint="node").add(node=Register(lambda context: None)).freeze()
    execution = Execution("work", limits=limits)
    execution.start(graph)
    return execution


@pytest.mark.parametrize("control", ["cancel", "timeout"])
def test_runner_wakes_on_cancel_or_deadline_without_polling(control) -> None:
    """Runner 只在任务完成、取消或 deadline 时醒来，而不是周期性检查。

    Args:
        control: cancel 由调用方取消，timeout 由 Graph 总时限到期。
    """

    timeout = 0.4 if control == "timeout" else None
    execution = started_execution(ExecutionLimits(timeout=timeout))
    checks = 0
    wait_timeout = execution.wait_timeout

    def counted() -> float | None:
        """统计 Runner 计算等待预算的次数。"""

        nonlocal checks
        checks += 1
        return wait_timeout()

    execution.wait_timeout = counted  # type: ignore[method-assign]
    entered = ThreadEvent()
    errors: list[BaseException] = []

    async def wait_forever() -> None:
        """在 Runner loop 上无限等待，直到被取消。"""

        entered.set()
        await asyncio.Event().wait()

    def resolve() -> None:
        """在独立线程中同步等待协程并记录控制异常。"""

        try:
            runner.resolve(wait_forever(), execution=execution)
        except BaseException as exc:
            errors.append(exc)

    runner = LocalRunner()
    thread = Thread(target=resolve)
    try:
        thread.start()
        assert entered.wait(1)
        time.sleep(0.3)
        # 50ms 轮询会在 0.3 秒内检查约 6 次。
        assert checks <= 2
        requested = time.monotonic()
        if control == "cancel":
            execution.cancel()
        thread.join(2)
        assert not thread.is_alive()
        if control == "cancel":
            assert time.monotonic() - requested < 0.5
    finally:
        runner.close()
    expected = ExecutionCancelledError if control == "cancel" else ExecutionTimeoutError
    assert len(errors) == 1 and isinstance(errors[0], expected)
    assert checks <= 3


def test_runner_close_is_bounded_when_coroutine_suppresses_cancellation(
    monkeypatch, caplog
) -> None:
    """协程抑制取消时 close 有界返回并记录警告，loop 线程在协程结束后退出。

    Args:
        monkeypatch: 缩短 close 等待时限。
        caplog: 捕获 Runner 警告。
    """

    monkeypatch.setattr(runner_module, "_CLOSE_JOIN_TIMEOUT", 0.05)
    entered, release = ThreadEvent(), ThreadEvent()
    results: list[object] = []

    async def stubborn() -> str:
        """吞掉一次取消，直到测试放行后才结束。"""

        entered.set()
        with suppress(asyncio.CancelledError):
            await asyncio.sleep(10)
        while not release.is_set():
            await asyncio.sleep(0.005)
        return "late"

    before = set(live_threads())
    runner = LocalRunner()
    (loop_thread,) = (
        thread
        for thread in live_threads()
        if thread not in before and thread.name == "interlace-async-runner"
    )
    waiter = Thread(target=lambda: results.append(runner.resolve(stubborn())))
    waiter.start()
    assert entered.wait(1)
    with caplog.at_level(logging.WARNING, logger="interlace.engine.runner"):
        started = time.monotonic()
        runner.close()
        assert time.monotonic() - started < 1
    assert "did not exit" in caplog.text
    assert loop_thread.is_alive()
    release.set()
    waiter.join(2)
    loop_thread.join(2)
    assert not loop_thread.is_alive()
    assert results == ["late"]


def test_repeated_error_reraise_does_not_grow_traceback() -> None:
    """多次等待同一失败 execution 时，每次抛出的 traceback 深度相同。"""

    execution = started_execution()
    try:
        raise ValueError("boom")
    except ValueError as exc:
        execution.fail(exc)

    depths = []
    for _ in range(5):
        with pytest.raises(ValueError, match="boom") as captured:
            execution.result()
        depths.append(len(traceback.extract_tb(captured.value.__traceback__)))
        with pytest.raises(ValueError, match="boom") as captured:
            tuple(execution)
        depths.append(len(traceback.extract_tb(captured.value.__traceback__)))
    assert len(set(depths[0::2])) == 1
    assert len(set(depths[1::2])) == 1


def test_huge_finite_timeouts_do_not_overflow_thread_waits() -> None:
    """远超线程等待上限的有限时限按“近似不限时”工作，不以 OverflowError 失败。"""

    release = ThreadEvent()

    class Slow(Node):
        """异步等待测试放行，使 Graph 与 Node 时限都参与等待。

        Attributes:
            timeout: 超过 threading.TIMEOUT_MAX 的 Node 时限秒数。
        """

        timeout = 1e12

        async def execute(self, inputs, context):
            """等待放行后返回输入。"""

            del context
            await asyncio.to_thread(release.wait, 5)
            return Output(inputs["default"])

    with Runtime() as runtime:
        runtime.register("slow", Graph(entrypoint="slow").add(slow=Slow()))
        execution = runtime.start("slow", 1, timeout=1e10)
        Thread(target=lambda: (time.sleep(0.05), release.set())).start()
        assert execution.wait(1e10)
        assert execution.result() == (Output(1),)


def test_execution_errors_raised_outside_engine_wrapping_get_attribution() -> None:
    """Node 直接抛出的 ExecutionError 与入口输入错误同样带有 graph 与 node 归属。"""

    class Custom(Node):
        """直接抛出领域侧 ExecutionError。

        Attributes:
            input_ports: 整数输入端口。
        """

        input_ports = Ports(default=int)

        def execute(self, inputs, context):
            """抛出未设置归属的 ExecutionError。"""

            del inputs, context
            raise ExecutionError("custom")

    with Runtime() as runtime:
        runtime.register("custom", Graph(entrypoint="node").add(node=Custom()))
        with pytest.raises(ExecutionError, match="custom") as raised:
            runtime.run("custom", 1)
        assert (raised.value.graph, raised.value.node) == ("custom", "node")
        with pytest.raises(PortValueTypeError) as invalid:
            runtime.run("custom", "not an int")
        assert (invalid.value.graph, invalid.value.node) == ("custom", "node")


def test_hook_signals_from_context_factory_are_rejected() -> None:
    """context_factory 抛出 StopGraph 不能被当作正常停止，而是以 Hook 契约错误失败。"""

    def factory(base, inputs):
        """错误地在工厂中抛出 Hook 专用信号。"""

        del base, inputs
        raise StopGraph(Output(7))

    class Echo(Node):
        """不会执行的节点。"""

        def execute(self, inputs, context):
            """返回输入。"""

            del context
            return Output(inputs["default"])

    with Runtime() as runtime:
        runtime.register(
            "factory",
            Graph(entrypoint="node", context_factory=factory).add(node=Echo()),
        )
        with pytest.raises(HookExecutionError, match="StopGraph"):
            runtime.run("factory", 1)


def test_closing_stream_from_another_thread_ends_blocked_reader() -> None:
    """读取方阻塞等待时，另一线程关闭同一订阅，读取方正常结束而不是抛 KeyError。"""

    release = ThreadEvent()

    class Wait(Node):
        """直到测试放行才结束。"""

        def execute(self, inputs, context):
            """等待放行。"""

            del inputs, context
            assert release.wait(5)

    with Runtime() as runtime:
        runtime.register("wait", Graph(entrypoint="node").add(node=Wait()))
        stream = runtime.iter("wait", 1)
        outcome: list[object] = []

        def read() -> None:
            """阻塞读取并记录结束方式。"""

            try:
                outcome.append(next(stream))
            except StopIteration:
                outcome.append("stopped")
            except BaseException as exc:
                outcome.append(exc)

        reader = Thread(target=read)
        reader.start()
        deadline = time.monotonic() + 5
        while not runtime.executions()[-1].steps:
            assert time.monotonic() < deadline
            time.sleep(0.005)
        time.sleep(0.05)
        stream.close()
        reader.join(5)
        release.set()
        assert outcome == ["stopped"]


def test_cancellation_cleanup_failure_is_logged(caplog) -> None:
    """取消后协程清理阶段的失败不替换控制异常，但写入错误日志。

    Args:
        caplog: 日志捕获夹具。
    """

    entered = ThreadEvent()

    class Leaky(Node):
        """清理阶段失败的异步节点。"""

        async def execute(self, inputs, context):
            """等待被取消，并在 finally 中失败。"""

            del inputs, context
            try:
                entered.set()
                await asyncio.sleep(10)
            finally:
                raise RuntimeError("cleanup broke")

    with Runtime() as runtime:
        runtime.register("leaky", Graph(entrypoint="node").add(node=Leaky()))
        with caplog.at_level(logging.ERROR, logger="interlace.engine.runner"):
            execution = runtime.start("leaky", 1)
            assert entered.wait(5)
            execution.cancel()
            assert execution.wait(5)
    assert execution.status is ExecutionStatus.CANCELLED
    assert any(
        record.exc_info and str(record.exc_info[1]) == "cleanup broke"
        for record in caplog.records
    )
