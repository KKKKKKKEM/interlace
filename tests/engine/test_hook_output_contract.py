"""逐项 Hook 输出转换、惰性恢复和资源关闭的契约测试。"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Iterator, Mapping
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
from interlace.engine.hooks import NodeCall, NodeHook, Outputs, StopGraph


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


def test_async_item_hooks_recover_original_generator_error_off_source_thread() -> None:
    """异步逐项转换与恢复共享异步线程，同步生成器保留独立线程和原始异常。"""

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

    class Recover(NodeHook):
        """异步转换输出，并按原始异常身份提供恢复输出。"""

        async def exit(self, call: NodeCall, output: Output) -> Output:
            """在异步循环中对当前单项输出进行转换。

            Args:
                call: 当前节点调用身份，本场景不修改。
                output: Engine 已经读取的当前输出。

            Returns:
                数值扩大十倍的新输出。
            """

            del call
            await asyncio.sleep(0)
            hook_threads.append(get_ident())
            converted.append(output.value)
            return Output(output.value * 10)

        async def error(self, call: NodeCall, error: Exception) -> Output:
            """保留原始异常证据并异步产生恢复输出。

            Args:
                call: 当前节点调用身份，本场景不修改。
                error: 同步源迭代时发生的原始领域异常。

            Returns:
                随后同样经过逐项出口转换的恢复输出。
            """

            del call
            await asyncio.sleep(0)
            hook_threads.append(get_ident())
            errors.append(error)
            return Output(2)

    with Runtime() as runtime:
        runtime.register("recover", Graph(entrypoint="source").add(source=Producer()))
        runtime.attach(Recover(), graph="recover")
        assert tuple(runtime.iter("recover")) == (Output(10), Output(20))

    assert errors == [domain_error]
    assert errors[0] is domain_error
    assert converted == [1, 2]
    assert len(set(source_threads)) == len(set(hook_threads)) == 1
    assert source_threads[0] != hook_threads[0]


@pytest.mark.parametrize("stop", [True, False], ids=["stop-graph", "exit-error"])
def test_hook_termination_closes_source_cursor_once_without_reading_ahead(stop) -> None:
    """Hook 在当前项终止执行时关闭源游标，并且不预读剩余输出。

    Args:
        stop: 为 True 时通过 StopGraph 终止，否则在出口抛出业务异常。
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

    class Terminate(NodeHook):
        """在取得第一项输出后停止 Graph 或终止当前节点。"""

        def exit(self, call: NodeCall, output: Output) -> Outputs:
            """在当前项边界终止，禁止 Engine 继续推进源。

            Args:
                call: 当前节点调用身份，本场景不修改。
                output: 已从源取得的第一项输出。

            Raises:
                StopGraph: 本场景选择整图短路成功。
                ValueError: 本场景选择出口转换失败。
            """

            del call, output
            if stop:
                raise StopGraph(Output(9))
            raise domain_error

    with Runtime() as runtime:
        runtime.register("cursor", Graph(entrypoint="source").add(source=Producer()))
        runtime.attach(Terminate(), graph="cursor")
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
def test_unbounded_source_times_out_without_hook_error_recovery(node_timeout) -> None:
    """无自然结束条件的输出源响应执行预算，控制异常绕过业务恢复。

    Args:
        node_timeout: 为 True 时使用节点预算，否则使用整图预算。
    """

    abort = ThreadEvent()
    closed = ThreadEvent()
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

    class Recover(NodeHook):
        """记录不应该接收到的控制异常恢复调用。"""

        def error(self, call: NodeCall, error: Exception) -> Output:
            """记录恢复尝试，以便断言控制异常没有进入此处。

            Args:
                call: 当前节点调用身份，本场景不修改。
                error: Hook 收到的待恢复异常。

            Returns:
                可被测试识别的恢复输出。
            """

            del call
            recovered.append(error)
            return Output("unexpected recovery")

    with Runtime() as runtime:
        runtime.register("unbounded", Graph(entrypoint="source").add(source=Producer()))
        runtime.attach(Recover(), graph="unbounded")
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
    assert recovered == []


@pytest.mark.parametrize(
    "error_type", [ExecutionCancelledError, StepLimitExceededError]
)
def test_generator_control_signal_bypasses_hook_recovery(error_type) -> None:
    """生成器显式抛出的控制异常也不会被逐项 Hook 当作业务错误恢复。

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

    class Recover(NodeHook):
        """记录误入业务恢复分支的控制异常。"""

        def error(self, call: NodeCall, error: Exception) -> Output:
            """记录不应发生的恢复尝试。

            Args:
                call: 当前节点调用身份，本场景不修改。
                error: 被错误传入的控制异常。

            Returns:
                可用于识别错误恢复行为的输出。
            """

            del call
            recovered.append(error)
            return Output(2)

    with Runtime() as runtime:
        runtime.register("control", Graph(entrypoint="source").add(source=Producer()))
        runtime.attach(Recover(), graph="control")
        stream = runtime.iter("control")
        assert next(stream) == Output(1)
        with pytest.raises(error_type) as captured:
            next(stream)
        assert captured.value is control_error

    assert recovered == []


def test_item_hooks_drop_expand_and_close_each_output_before_source_advances() -> None:
    """逐项转换可保留、丢弃与展开，并在读取下个源项前释放当前展开游标。"""

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

    class Transform(NodeHook):
        """覆盖单项保留、两种丢弃和资源游标展开的出口转换。"""

        def exit(self, call: NodeCall, output: Output) -> Outputs:
            """对当前项独立转换，转换输出不再次交回同一个 Hook。

            Args:
                call: 当前节点调用身份，本场景不修改。
                output: 等待转换的单项源输出。

            Returns:
                单项输出、展开游标或代表丢弃的空结果。
            """

            del call
            converted.append(output.value)
            if output.value == 0:
                return None
            if output.value == 1:
                return ()
            if output.value == 2:
                return Output(20)
            cursor = CloseTrackedCursor([Output(output.value * 10), Output(99)])
            expansions.append(cursor)
            return cursor

    class Observe(NodeHook):
        """记录内层转换之后抵达外层 Hook 的实际输出。"""

        def exit(self, call: NodeCall, output: Output) -> Output:
            """记录已展开且未丢弃的输出并继续交付。

            Args:
                call: 当前节点调用身份，本场景不修改。
                output: 内层 Hook 已经转换的单项输出。

            Returns:
                保持原值的当前输出。
            """

            del call
            observed.append(output.value)
            return output

    with Runtime() as runtime:
        runtime.register("transform", Graph(entrypoint="source").add(source=Producer()))
        runtime.attach(Observe(), graph="transform")
        runtime.attach(Transform(), graph="transform")
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
