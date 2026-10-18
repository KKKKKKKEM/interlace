"""NODE_CALL 中间件的逐项输出改写、惰性恢复、控制异常与资源关闭契约测试。"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Iterator, Mapping
from contextlib import closing
from threading import Event as ThreadEvent
from threading import get_ident
from typing import Any

import pytest

from interlace import Context, Graph, Node, Output, Runtime
from interlace.engine.errors import (
    ExecutionCancelledError,
    ExecutionError,
    ExecutionTimeoutError,
    NodeTimeoutError,
    StepLimitExceededError,
)
from interlace.plugins.points import NODE_CALL, NodeCall, Outputs, StopGraph


class CloseTrackedCursor(Iterator[Output]):
    """记录推进和关闭次数的资源游标。

    Attributes:
        _values: 当前游标独立持有的输出迭代器。
        next_calls: 包括读取结束标记在内的推进调用次数。
        close_calls: 资源关闭调用次数，允许断言没有重复关闭。
    """

    def __init__(self, values: Iterable[Output]) -> None:
        """建立游标并初始化资源操作计数。

        Args:
            values: 游标按顺序交付的输出集合。
        """

        self._values = iter(values)
        self.next_calls = 0
        self.close_calls = 0

    def __next__(self) -> Output:
        """记录推进次数后读取下一项输出。

        Returns:
            游标下一项输出。

        Raises:
            StopIteration: 游标已经耗尽。
        """

        self.next_calls += 1
        return next(self._values)

    def close(self) -> None:
        """记录一次关闭，保留重复关闭证据供测试检查。"""

        self.close_calls += 1


def test_middleware_waits_async_work_off_source_thread_and_recovers_original_error() -> (
    None
):
    """中间件经 call.wait 在异步线程完成转换与恢复，同步生成器保留独立线程和原始异常。"""

    source_threads: list[int] = []
    hook_threads: list[int] = []
    converted: list[int] = []
    errors: list[Exception] = []
    domain_error = ValueError("source failed after first output")

    class Producer(Node):
        """产生前缀输出后抛出预设领域异常的同步源。"""

        def execute(
            self, inputs: Mapping[str, Any], context: Context
        ) -> Iterator[Output]:
            """记录每次推进线程并在已交付前缀之后失败。

            Args:
                inputs: 当前执行入口输入，本场景不使用。
                context: 当前执行上下文，本场景不使用。

            Yields:
                发生领域错误前的一项输出。

            Raises:
                ValueError: 当前测试预设的原始领域异常。
            """

            del inputs, context
            source_threads.append(get_ident())
            yield Output(1)
            source_threads.append(get_ident())
            raise domain_error

    async def convert(value: int) -> Output:
        """在异步循环中转换单项输出。

        Args:
            value: 当前输出值。

        Returns:
            数值扩大十倍的新输出。
        """

        await asyncio.sleep(0)
        hook_threads.append(get_ident())
        converted.append(value)
        return Output(value * 10)

    async def fallback(error: Exception) -> Output:
        """保留原始异常证据并异步产生恢复输出。

        Args:
            error: 同步源迭代时发生的原始领域异常。

        Returns:
            随后同样经过转换的恢复输出。
        """

        await asyncio.sleep(0)
        hook_threads.append(get_ident())
        errors.append(error)
        return Output(2)

    def recover(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """逐项转换内层输出，内层业务失败时以恢复输出继续。

        Args:
            call: 当前节点调用。
            call_next: 调用内层的函数。

        Yields:
            转换后的输出与恢复输出。
        """

        try:
            for output in call_next(call):
                yield call.wait(convert(output.value))
        except ValueError as error:
            yield call.wait(convert(call.wait(fallback(error)).value))

    with Runtime() as runtime:
        runtime.register("recover", Graph(entrypoint="source").add(source=Producer()))
        runtime.hooks.on(NODE_CALL, recover, graph="recover")
        assert tuple(runtime.iter("recover")) == (Output(10), Output(20))

    assert errors == [domain_error]
    assert errors[0] is domain_error
    assert converted == [1, 2]
    assert len(set(source_threads)) == len(set(hook_threads)) == 1
    assert source_threads[0] != hook_threads[0]


@pytest.mark.parametrize("stop", [True, False], ids=["stop-graph", "exit-error"])
def test_middleware_termination_closes_source_cursor_once_without_reading_ahead(
    stop,
) -> None:
    """中间件在当前项终止执行时关闭源游标，并且不预读剩余输出。

    Args:
        stop: 为 True 时通过 StopGraph 终止，否则在改写当前项时抛出业务异常。
    """

    cursor = CloseTrackedCursor([Output(1), Output(2)])
    domain_error = ValueError("exit failed")

    class Producer(Node):
        """向 Engine 移交当前测试独立拥有的资源游标。"""

        def execute(self, inputs: Mapping[str, Any], context: Context) -> Outputs:
            """返回尚未推进的资源游标。

            Args:
                inputs: 当前入口输入，本场景不使用。
                context: 当前执行上下文，本场景不使用。

            Returns:
                由 Engine 接管关闭职责的输出游标。
            """

            del inputs, context
            return cursor

    def terminate(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """在取得第一项输出后停止 Graph 或终止当前节点。

        Args:
            call: 当前节点调用。
            call_next: 调用内层的函数。

        Yields:
            不产生任何输出。

        Raises:
            StopGraph: 本场景选择整图短路成功。
            ValueError: 本场景选择改写失败。
        """

        for _ in call_next(call):
            if stop:
                raise StopGraph(Output(9))
            raise domain_error
        yield from ()

    with Runtime() as runtime:
        runtime.register("cursor", Graph(entrypoint="source").add(source=Producer()))
        runtime.hooks.on(NODE_CALL, terminate, graph="cursor")
        if stop:
            assert runtime.run("cursor") == (Output(9),)
        else:
            with pytest.raises(ExecutionError) as captured:
                runtime.run("cursor")
            assert captured.value.__cause__ is domain_error

    assert cursor.next_calls == 1
    assert cursor.close_calls == 1


def test_cancellation_closes_returned_cursor_before_first_advance() -> None:
    """取消与返回值交接重叠时，未推进的游标仍由 Engine 关闭一次。"""

    cursor = CloseTrackedCursor([Output(1)])
    acquired = ThreadEvent()
    release = ThreadEvent()

    class Producer(Node):
        """在游标资源已经获得但尚未返回时等待取消的同步源。"""

        def execute(self, inputs: Mapping[str, Any], context: Context) -> Outputs:
            """允许测试在资源返回前注入取消请求。

            Args:
                inputs: 当前入口输入，本场景不使用。
                context: 当前执行上下文，本场景不使用。

            Returns:
                取消后仍须关闭的未推进资源游标。
            """

            del inputs, context
            acquired.set()
            release.wait(2)
            return cursor

    with Runtime() as runtime:
        runtime.register("cancel", Graph(entrypoint="source").add(source=Producer()))
        execution = runtime.start("cancel")
        try:
            assert acquired.wait(1)
            assert execution.cancel()
        finally:
            release.set()
        with pytest.raises(ExecutionCancelledError):
            execution.result(1)

    assert cursor.next_calls == 0
    assert cursor.close_calls == 1


@pytest.mark.parametrize("node_timeout", [False, True], ids=["graph", "node"])
def test_unbounded_source_timeout_cannot_be_swallowed_by_middleware(
    node_timeout,
) -> None:
    """无自然结束条件的输出源响应执行预算；中间件捕获控制异常后也无法交付替代输出。

    Args:
        node_timeout: 为 True 时使用节点预算，否则使用整图预算。
    """

    abort = ThreadEvent()
    closed = ThreadEvent()
    delivered: list[object] = []
    recovered: list[Exception] = []

    class Producer(Node):
        """持续逐项产出直到控制边界终止的同步源。

        Attributes:
            timeout: 单次节点预算，单位秒；整图预算场景中为 None。
        """

        timeout = 0.02 if node_timeout else None

        def execute(
            self, inputs: Mapping[str, Any], context: Context
        ) -> Iterator[Output]:
            """持续产出，并在停止后记录源资源已释放。

            Args:
                inputs: 当前入口输入，本场景不使用。
                context: 当前执行上下文，本场景不使用。

            Yields:
                按需交付的连续输出。
            """

            del inputs, context
            try:
                while not abort.is_set():
                    yield Output(1)
            finally:
                closed.set()

    def observe(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """外层记录实际交付的输出。

        Args:
            call: 当前节点调用。
            call_next: 调用内层的函数。

        Yields:
            内层输出。
        """

        for output in call_next(call):
            delivered.append(output.value)
            yield output

    def recover(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """记录收到的异常并尝试以替代输出恢复。

        Args:
            call: 当前节点调用。
            call_next: 调用内层的函数。

        Yields:
            内层输出，或失败时的替代输出。
        """

        try:
            yield from call_next(call)
        except Exception as error:
            recovered.append(error)
            yield Output("unexpected recovery")

    with Runtime() as runtime:
        runtime.register("unbounded", Graph(entrypoint="source").add(source=Producer()))
        runtime.hooks.on(NODE_CALL, observe, graph="unbounded")
        runtime.hooks.on(NODE_CALL, recover, graph="unbounded")
        execution = runtime.start("unbounded", timeout=None if node_timeout else 0.02)
        try:
            expected = NodeTimeoutError if node_timeout else ExecutionTimeoutError
            with pytest.raises(expected):
                execution.result(1)
        finally:
            abort.set()
            execution.cancel()
            assert execution.wait(1)

    assert closed.is_set()
    assert "unexpected recovery" not in delivered
    assert all(isinstance(error, expected) for error in recovered)


@pytest.mark.parametrize(
    "error_type", [ExecutionCancelledError, StepLimitExceededError]
)
def test_generator_control_signal_cannot_be_swallowed_by_middleware(error_type) -> None:
    """生成器显式抛出的控制异常即使被中间件捕获，也按原实例传播且替代输出不交付。

    Args:
        error_type: 需要按原实例传播的执行控制异常类型。
    """

    control_error = error_type("stop source")
    recovered: list[Exception] = []

    class Producer(Node):
        """交付前缀后显式抛出控制异常的同步源。"""

        def execute(
            self, inputs: Mapping[str, Any], context: Context
        ) -> Iterator[Output]:
            """让控制异常发生在第二次惰性推进期间。

            Args:
                inputs: 当前入口输入，本场景不使用。
                context: 当前执行上下文，本场景不使用。

            Yields:
                控制异常发生前的已交付输出。

            Raises:
                ExecutionCancelledError: 本用例选择取消控制信号。
                StepLimitExceededError: 本用例选择步数控制信号。
            """

            del inputs, context
            yield Output(1)
            raise control_error

    def recover(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """尝试把内层全部普通异常转换为替代输出。

        Args:
            call: 当前节点调用。
            call_next: 调用内层的函数。

        Yields:
            内层输出，或失败时的替代输出。
        """

        try:
            yield from call_next(call)
        except Exception as error:
            recovered.append(error)
            yield Output(2)

    with Runtime() as runtime:
        runtime.register("control", Graph(entrypoint="source").add(source=Producer()))
        runtime.hooks.on(NODE_CALL, recover, graph="control")
        stream = runtime.iter("control")
        assert next(stream) == Output(1)
        with pytest.raises(error_type) as captured:
            next(stream)
        assert captured.value is control_error

    assert recovered == [control_error]


def test_middleware_drops_expands_and_closes_each_output_before_source_advances() -> (
    None
):
    """中间件可保留、丢弃与展开输出，链路保持惰性：读取下个源项前当前展开已消费并关闭。"""

    converted: list[int] = []
    observed: list[int] = []
    expansions: list[CloseTrackedCursor] = []

    class Producer(Node):
        """在各输出边界验证前一项展开资源已关闭的同步源。"""

        def execute(
            self, inputs: Mapping[str, Any], context: Context
        ) -> Iterator[Output]:
            """按请求数量产出，并验证 Engine 没有积压已经耗尽的资源。

            Args:
                inputs: default 指定当前执行应产生的输出项数。
                context: 当前执行上下文，本场景不使用。

            Yields:
                从零开始递增的当前源输出。
            """

            del context
            for index in range(inputs["default"]):
                assert all(cursor.close_calls == 1 for cursor in expansions)
                yield Output(index)

    def transform(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """对每项独立改写：丢弃、保留或展开为资源游标。

        Args:
            call: 当前节点调用。
            call_next: 调用内层的函数。

        Yields:
            改写后的输出。
        """

        for output in call_next(call):
            converted.append(output.value)
            if output.value in (0, 1):
                continue
            if output.value == 2:
                yield Output(20)
                continue
            cursor = CloseTrackedCursor([Output(output.value * 10), Output(99)])
            expansions.append(cursor)
            with closing(cursor):
                yield from cursor

    def observe(call: NodeCall, call_next: Any) -> Iterator[Output]:
        """记录内层改写之后抵达外层的实际输出。

        Args:
            call: 当前节点调用。
            call_next: 调用内层的函数。

        Yields:
            保持原值的输出。
        """

        for output in call_next(call):
            observed.append(output.value)
            yield output

    with Runtime() as runtime:
        runtime.register("transform", Graph(entrypoint="source").add(source=Producer()))
        runtime.hooks.on(NODE_CALL, observe, graph="transform")
        runtime.hooks.on(NODE_CALL, transform, graph="transform")
        assert runtime.run("transform", 5) == (
            Output(20),
            Output(30),
            Output(99),
            Output(40),
            Output(99),
        )
        assert converted == [0, 1, 2, 3, 4]
        assert observed == [20, 30, 99, 40, 99]
        assert all(cursor.close_calls == 1 for cursor in expansions)
        assert runtime.run("transform", 0) == ()

    assert converted == [0, 1, 2, 3, 4]
    assert observed == [20, 30, 99, 40, 99]
