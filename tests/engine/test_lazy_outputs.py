"""惰性节点输出、Hook 边界和完成观测的契约测试。"""

from collections.abc import Iterator, Mapping
from threading import Event as ThreadEvent
from typing import Any

import pytest

from interlace import Context, Graph, Node, Output, Ports, Runtime
from interlace.engine.errors import (
    ExecutionCancelledError,
    HookExecutionError,
    PortValueTypeError,
)
from interlace.engine.hooks import NodeCall, NodeHook, ShortCircuit, StopGraph
from interlace.engine.observation import RuntimeEventKind


@pytest.mark.parametrize("with_hook", [False, True])
def test_first_output_arrives_before_generator_finishes(with_hook: bool) -> None:
    """首项输出到达后才允许生产下一项，且 Hook 不提前耗尽源。

    Args:
        with_hook: 是否通过惰性出口 Hook 转换每项输出。
    """

    received = ThreadEvent()

    class Producer(Node):
        """等待首项被读取后继续产出的节点。"""

        def execute(
            self, inputs: Mapping[str, Any], context: Context
        ) -> Iterator[Output]:
            """先产出首项，再等待消费者确认。

            Args:
                inputs: 本次入口值，此测试不使用。
                context: 当前执行上下文，此测试不使用。

            Yields:
                按顺序产生的两个整数。
            """

            yield Output(1)
            assert received.wait(1), "first output was buffered until exhaustion"
            yield Output(2)

    class Transform(NodeHook):
        """保持惰性的逐项输出转换。"""

        def exit(self, call: NodeCall, output: Output) -> Output:
            """转换当前项，不读取下一项。

            Args:
                call: 当前节点调用。
                output: Engine 已读取的当前输出。

            Returns:
                将当前整数翻倍的输出。
            """

            return Output(output.value * 2)

    with Runtime() as runtime:
        runtime.register("lazy", Graph(entrypoint="source").add(source=Producer()))
        if with_hook:
            runtime.attach(Transform())
        stream = runtime.iter("lazy", output_buffer=1)
        try:
            assert next(stream) == Output(2 if with_hook else 1)
        finally:
            received.set()
        assert tuple(stream) == (Output(4 if with_hook else 2),)


@pytest.mark.parametrize("signal", [ShortCircuit, StopGraph])
def test_generator_cannot_raise_hook_control(signal: type[BaseException]) -> None:
    """迭代阶段的节点控制信号仍被拒绝，已交付输出保留。

    Args:
        signal: 节点不得自行发出的 Hook 控制信号类型。
    """

    class Invalid(Node):
        """在第二次推进时冒充 Hook 的节点。

        Attributes:
            output_ports: 仅允许默认端口的整数输出。
        """

        output_ports = Ports(default=int)

        def execute(
            self, inputs: Mapping[str, Any], context: Context
        ) -> Iterator[Output]:
            """先发布合法数据，再发出非法控制信号。

            Args:
                inputs: 本次入口值，此测试不使用。
                context: 当前执行上下文，此测试不使用。

            Yields:
                第一项合法整数。

            Raises:
                BaseException: 用于验证节点不能发出的 Hook 控制信号。
            """

            yield Output(1)
            raise signal(Output("escape", "undeclared"))

    with Runtime() as runtime:
        runtime.register("invalid", Graph(entrypoint="source").add(source=Invalid()))
        stream = runtime.iter("invalid")
        assert next(stream) == Output(1)
        with pytest.raises(HookExecutionError, match="only be raised by a hook"):
            next(stream)


def test_lazy_output_backpressure_and_cancellation_close_generator() -> None:
    """背压限制生成器预取，取消时关闭尚未耗尽的输出流。"""

    second = ThreadEvent()
    third = ThreadEvent()
    cleaned = ThreadEvent()

    class Producer(Node):
        """记录预取进度并在关闭时清理的节点。"""

        def execute(
            self, inputs: Mapping[str, Any], context: Context
        ) -> Iterator[Output]:
            """产生三项数据并记录清理完成。

            Args:
                inputs: 本次入口值，此测试不使用。
                context: 当前执行上下文，此测试不使用。

            Yields:
                三个整数，第二项应受输出背压阻塞。
            """

            try:
                yield Output(1)
                second.set()
                yield Output(2)
                third.set()
                yield Output(3)
            finally:
                cleaned.set()

    with Runtime() as runtime:
        runtime.register("lazy", Graph(entrypoint="source").add(source=Producer()))
        stream = runtime.iter("lazy", output_buffer=1)
        execution = runtime.executions()[-1]
        try:
            assert second.wait(1)
            assert not third.wait(0.05)
        finally:
            execution.cancel()
            stream.close()  # type: ignore[attr-defined]
        with pytest.raises(ExecutionCancelledError):
            execution.result(1)
        assert cleaned.is_set()


def test_invalid_output_marks_node_observation_failed() -> None:
    """输出类型校验失败必须反映在节点完成事件中。"""

    events = []

    class Invalid(Node):
        """返回错误类型的节点。

        Attributes:
            output_ports: 默认输出必须为整数。
        """

        output_ports = Ports(default=int)

        def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
            """返回与声明不一致的字符串。

            Args:
                inputs: 本次入口值，此测试不使用。
                context: 当前执行上下文，此测试不使用。

            Returns:
                故意违反输出类型契约的数据。
            """

            return Output("wrong")

    with Runtime() as runtime:
        runtime.observe_runtime(events.append)
        runtime.register("invalid", Graph(entrypoint="source").add(source=Invalid()))
        with pytest.raises(PortValueTypeError):
            runtime.run("invalid")

    finished = next(
        event for event in events if event.kind is RuntimeEventKind.NODE_FINISHED
    )
    assert finished.status == "failed"
    assert finished.error_type == "PortValueTypeError"
