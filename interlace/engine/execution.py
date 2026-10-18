"""单次 Graph execution 的限制、状态和协作式控制。"""

from __future__ import annotations

import asyncio
import enum
import itertools
import math
import time
from collections.abc import AsyncIterator, Callable, Generator, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import TIMEOUT_MAX, RLock
from types import TracebackType
from typing import Any, NoReturn
from uuid import uuid4

from .._validation import require_non_empty_string, validate_timeout
from .core import Output
from .errors import (
    ExecutionCancelledError,
    ExecutionTimeoutError,
    NodeTimeoutError,
    QueueLimitExceededError,
    StepLimitExceededError,
)
from .execution_resources import (
    ExecutionNotifier,
    LocalExecutionNotifier,
    MemoryOutputStore,
    OutputStore,
)
from .graph import Graph


def _validate_duration(value: float | None, label: str) -> None:
    """校验有限正秒数，None 表示不限时。

    Raises:
        TypeError: value 不是 int/float（bool 除外）且不是 None。
        ValueError: value 不是有限正数。
    """

    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number or None")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{label} must be a finite number greater than zero")


@dataclass(frozen=True, slots=True)
class ExecutionLimits:
    """一次 Graph execution 的步数、总时长与待消费输入上限；默认均不限制。

    Attributes:
        max_steps: Node firing 次数上限，0 表示不限制。
        timeout: 整次 execution 的时长上限，单位秒；None 表示不限制。
        max_queued: Graph 内全部输入端口等待消费的 token 总数上限，0 表示不限制。
    """

    max_steps: int = 0
    timeout: float | None = None
    max_queued: int = 0

    def __post_init__(self) -> None:
        """校验步数与队列上限为非负整数、timeout 为有限正秒数或 None。

        Raises:
            TypeError: max_steps 或 max_queued 不是 int，或 timeout 不是数字且不是 None。
            ValueError: max_steps 或 max_queued 为负数，或 timeout 不是有限正数。
        """

        for label, value in (
            ("max_steps", self.max_steps),
            ("max_queued", self.max_queued),
        ):
            if type(value) is not int:
                raise TypeError(f"{label} must be an integer")
            if value < 0:
                raise ValueError(f"{label} must not be negative")
        _validate_duration(self.timeout, "timeout")


class ExecutionStatus(str, enum.Enum):
    """一项 Execution 对调用方可见的生命周期状态。

    Attributes:
        PENDING: 执行已创建但尚未开始。
        RUNNING: Graph 正在执行。
        SUCCEEDED: 执行成功完成。
        FAILED: 执行因业务或基础设施异常失败。
        CANCELLED: 执行已取消。
        TIMED_OUT: 执行或节点超时。
        STEP_LIMITED: 执行达到步数限制。
        QUEUE_LIMITED: Graph 内待消费输入超过队列上限。
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    STEP_LIMITED = "step_limited"
    QUEUE_LIMITED = "queue_limited"

    @property
    def terminal(self) -> bool:
        """判断状态是否为不再继续运行的终态。

        Returns:
            PENDING 与 RUNNING 以外的状态返回 True。
        """

        return self not in (ExecutionStatus.PENDING, ExecutionStatus.RUNNING)


def terminal_status(error: BaseException | None) -> ExecutionStatus:
    """把执行终止原因映射为 Execution 终态。

    执行宿主可在调用 `Execution.succeed()`/`fail()` 之前用它确定即将写入的状态，
    `Execution.fail()` 使用同一映射。

    Args:
        error: 终止执行的异常；None 表示正常完成。

    Returns:
        None 对应 SUCCEEDED；取消、超时、步数与队列限制对应各自终态；其他异常为 FAILED。
    """

    if error is None:
        return ExecutionStatus.SUCCEEDED
    if isinstance(error, ExecutionCancelledError):
        return ExecutionStatus.CANCELLED
    if isinstance(error, (ExecutionTimeoutError, NodeTimeoutError)):
        return ExecutionStatus.TIMED_OUT
    if isinstance(error, StepLimitExceededError):
        return ExecutionStatus.STEP_LIMITED
    if isinstance(error, QueueLimitExceededError):
        return ExecutionStatus.QUEUE_LIMITED
    return ExecutionStatus.FAILED


class _OutputCursor:
    """一项输出订阅的共享部分：登记游标、关闭时解除背压订阅。

    Attributes:
        _execution: 订阅所属的 Execution。
        _stream_id: Execution 为本订阅分配的游标编号。
        _closed: 订阅是否已解除；解除后不再读取输出。
    """

    def __init__(self, execution: Execution) -> None:
        """在 Execution 上登记一个从索引 0 开始的新游标。"""

        # 登记失败时 __del__ 也会调用 close，因此先标记为已关闭。
        self._closed = True
        self._execution = execution
        self._stream_id = execution._attach_stream()
        self._closed = False

    def close(self) -> None:
        """解除订阅并唤醒因背压等待的生产者；重复调用无作用。"""

        if not self._closed:
            self._closed = True
            self._execution._detach_stream(self._stream_id)

    def __del__(self) -> None:
        """回收未关闭的订阅，避免其游标继续参与背压。"""

        self.close()


class _OutputIterator(_OutputCursor, Iterator[Output]):
    """同步读取 terminal Output 的独立订阅。"""

    def __next__(self) -> Output:
        """阻塞读取下一项输出；结束或抛错时解除订阅。

        Raises:
            StopIteration: 执行成功结束且已读完全部输出。
            BaseException: 执行失败时，读完已发布输出后重新抛出执行异常。
        """

        if self._closed:
            raise StopIteration
        try:
            output = self._execution._next_output(self._stream_id)
        except BaseException:
            self.close()
            raise
        if output is None:
            self.close()
            raise StopIteration
        return output


class _AsyncOutputIterator(_OutputCursor, AsyncIterator[Output]):
    """异步读取 terminal Output 的独立订阅，等待时不阻塞事件循环。"""

    def __aiter__(self) -> _AsyncOutputIterator:
        """返回自身。"""

        return self

    async def __anext__(self) -> Output:
        """异步读取下一项输出；结束、抛错或被取消时解除订阅。

        Raises:
            StopAsyncIteration: 执行成功结束且已读完全部输出。
            BaseException: 执行失败时，读完已发布输出后重新抛出执行异常。
        """

        if self._closed:
            raise StopAsyncIteration
        try:
            output = await self._execution._next_output_async(self._stream_id)
        except BaseException:
            self.close()
            raise
        if output is None:
            self.close()
            raise StopAsyncIteration
        return output

    async def aclose(self) -> None:
        """解除订阅，语义同 close。"""

        self.close()


class Execution:
    """单次 Graph execution 的统一句柄：状态、控制、等待与 terminal Output 流。

    状态读写受内部锁保护，可跨线程调用。完整输出保存在 OutputStore 中，可多次重放。

    Attributes:
        id: execution 标识，缺省为随机 UUID 字符串。
        graph: 目标 Graph 的注册名称。
        limits: 本次执行的步数与总时长限制。
        output_buffer: 每个活跃输出流允许的最大未读输出条数。
        created_at: 句柄创建的 UTC 时间。
        _condition: 保护全部可变状态的可重入锁；等待由 _notifier 完成。
        _store: 按追加顺序保存全部 terminal Output 的存储，由本句柄独占写入。
        _notifier: 状态、输出或游标变化时唤醒同步与异步等待方的通知器。
        _status: 当前生命周期状态。
        _started_at: 开始执行的 UTC 时间，未开始时为 None。
        _finished_at: 进入终态的 UTC 时间，未结束时为 None。
        _started_monotonic: 开始执行时的单调时钟读数，单位秒；未开始时为 None。
        _node_started_monotonic: 当前 firing 开始时的单调时钟读数，单位秒；空闲时为 None。
        _step_timeout: 当前 firing 的 Node timeout 秒数；空闲或不限时为 None。
        _node_timeouts: start 时绑定的各节点 timeout 快照；未绑定时为 None。
        _current_node: 当前 firing 的节点 ID，空闲时为 None。
        _steps: 已开始的 Node firing 次数。
        _error: 终止执行的异常；成功或未结束时为 None。
        _error_traceback: _error 进入终态时的原始 traceback，每次重新抛出都从它开始。
        _cancel_requested: 是否已接受取消请求。
        _cancel_listeners: cancel 被接受时同步调用的内部回调，按订阅令牌索引。
        _stream_counter: 分配输出游标编号的计数器。
        _stream_cursors: 活跃输出流编号到下次读取索引的映射，参与背压计算。
        _unbounded: 宿主关闭时置为 True，此后发布不再等待订阅读取，输出全部留在存储中；只置位不复位。
    """

    def __init__(
        self,
        graph: str,
        *,
        limits: ExecutionLimits | None = None,
        id: str | None = None,
        output_buffer: int = 64,
        output_store: OutputStore | None = None,
        notifier: ExecutionNotifier | None = None,
    ) -> None:
        """创建 PENDING 状态的执行句柄。

        Args:
            graph: 目标 Graph 的注册名称，必须是非空字符串。
            limits: 步数与总时长限制；None 使用不限制的默认值。
            id: execution 标识；None 时生成随机 UUID。
            output_buffer: 每个活跃输出流允许的最大未读输出条数，至少为 1。
            output_store: 空的追加式输出存储；None 使用内存存储。
            notifier: 版本化等待通知器；None 使用进程内默认实现。

        Raises:
            TypeError: limits、output_store 或 notifier 类型不符，或 output_store 非空。
            ValueError: graph 或 id 为空，或 output_buffer 不是正整数。
        """

        self.id = require_non_empty_string(
            str(uuid4()) if id is None else id,
            "execution id",
        )
        self.graph = require_non_empty_string(graph, "execution graph")
        if limits is None:
            limits = ExecutionLimits()
        if not isinstance(limits, ExecutionLimits):
            raise TypeError("limits must be ExecutionLimits or None")
        self.limits = limits
        if type(output_buffer) is not int or output_buffer < 1:
            raise ValueError("output_buffer must be an integer greater than zero")
        self.output_buffer = output_buffer
        self.created_at = datetime.now(timezone.utc)

        self._condition = RLock()
        self._store = MemoryOutputStore() if output_store is None else output_store
        self._notifier = LocalExecutionNotifier() if notifier is None else notifier
        if not isinstance(self._store, OutputStore) or len(self._store):
            raise TypeError("output_store must be an empty OutputStore")
        if not isinstance(self._notifier, ExecutionNotifier):
            raise TypeError("notifier must implement ExecutionNotifier")
        self._status = ExecutionStatus.PENDING
        self._started_at: datetime | None = None
        self._finished_at: datetime | None = None
        self._started_monotonic: float | None = None
        self._node_started_monotonic: float | None = None
        self._step_timeout: float | None = None
        self._node_timeouts: dict[str, float | None] | None = None
        self._current_node: str | None = None
        self._steps = 0
        self._error: BaseException | None = None
        self._error_traceback: TracebackType | None = None
        self._cancel_requested = False
        self._cancel_listeners: dict[object, Callable[[], None]] = {}
        self._stream_counter = itertools.count()
        self._stream_cursors: dict[int, int] = {}
        self._unbounded = False

    @property
    def status(self) -> ExecutionStatus:
        """当前生命周期状态。"""

        with self._condition:
            return self._status

    @property
    def started_at(self) -> datetime | None:
        """开始执行的 UTC 时间，尚未开始时为 None。"""

        with self._condition:
            return self._started_at

    @property
    def finished_at(self) -> datetime | None:
        """进入终态的 UTC 时间，尚未结束时为 None。"""

        with self._condition:
            return self._finished_at

    @property
    def current_node(self) -> str | None:
        """当前 firing 的节点 ID，没有 firing 进行时为 None。"""

        with self._condition:
            return self._current_node

    @property
    def steps(self) -> int:
        """已开始的 Node firing 次数。"""

        with self._condition:
            return self._steps

    @property
    def outputs(self) -> tuple[Output, ...] | None:
        """成功执行的完整 terminal Output；未处于 SUCCEEDED 时为 None。

        每次读取都从 OutputStore 物化完整 tuple。
        """

        with self._condition:
            if self._status is not ExecutionStatus.SUCCEEDED:
                return None
            return tuple(self._store[index] for index in range(len(self._store)))

    @property
    def error(self) -> BaseException | None:
        """终止执行的异常；成功或尚未结束时为 None。"""

        with self._condition:
            return self._error

    @property
    def cancel_requested(self) -> bool:
        """是否已接受取消请求。"""

        with self._condition:
            return self._cancel_requested

    @property
    def done(self) -> bool:
        """是否已进入终态。"""

        return self.status.terminal

    def cancel(self) -> bool:
        """请求协作式取消。

        PENDING 时立即进入 CANCELLED；RUNNING 时在下一次检查点抛出
        ExecutionCancelledError，等待中的异步调用会被立即唤醒并收到取消。

        Returns:
            请求被接受返回 True；执行已处于终态时返回 False。
        """

        with self._condition:
            if self._status.terminal:
                return False
            self._cancel_requested = True
            if self._status is ExecutionStatus.PENDING:
                error = ExecutionCancelledError(
                    f"execution {self.id!r} was cancelled before it started",
                    graph=self.graph,
                )
                self._finish_locked(ExecutionStatus.CANCELLED, error=error)
            self._notifier.notify()
            listeners = tuple(self._cancel_listeners.values())
        for listener in listeners:
            listener()
        return True

    def wait(self, timeout: float | None = None) -> bool:
        """阻塞等待终态，不改变 Execution。

        Args:
            timeout: 最长等待秒数；None 表示一直等待，0 表示立即检查。

        Returns:
            已进入终态返回 True；超时仍未结束返回 False。

        Raises:
            TypeError: timeout 不是数字且不是 None。
            ValueError: timeout 为负数或非有限值。
        """

        validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._condition:
                if self._status.terminal:
                    return True
                version = self._notifier.version
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
            # 超大有限时限超过线程等待上限时分段等待，循环会重新计算剩余时间。
            self._notifier.wait(
                version, None if remaining is None else min(remaining, TIMEOUT_MAX)
            )

    def result(self, timeout: float | None = None) -> tuple[Output, ...]:
        """等待终态并返回完整 terminal Output；失败时重新抛出原始执行异常。

        Args:
            timeout: 最长等待秒数；None 表示一直等待，0 表示立即检查。

        Returns:
            按发布顺序排列的全部 terminal Output。

        Raises:
            TimeoutError: timeout 内未进入终态。
            BaseException: 执行失败、取消、超时或达到步数限制时的原始异常。
        """

        if not self.wait(timeout):
            raise TimeoutError(f"execution {self.id!r} did not finish in time")
        with self._condition:
            if self._error is not None:
                self._raise_error_locked()
            outputs = self.outputs
            assert outputs is not None
            return outputs

    def __await__(self) -> Generator[Any, None, tuple[Output, ...]]:
        """异步等待 `result()`；取消等待方的 Task 会同时取消本次 execution。"""

        return self._await_result().__await__()

    def __iter__(self) -> Iterator[Output]:
        """创建同步 terminal Output 订阅。

        每个订阅从索引 0 重放已发布输出，再阻塞等待后续输出；成功结束时停止迭代，
        失败时在读完已发布输出后抛出执行异常。活跃订阅参与背压：任一订阅未读输出
        达到 `output_buffer` 时生产者等待，Node timeout 也计入这段等待。关闭或回收
        订阅即解除背压，但不会取消 execution。

        Returns:
            独立的同步输出迭代器。
        """

        return _OutputIterator(self)

    def __aiter__(self) -> _AsyncOutputIterator:
        """创建异步 terminal Output 订阅，重放、背压与结束语义同 `__iter__`。

        Returns:
            独立的异步输出迭代器，等待时不阻塞事件循环。
        """

        return _AsyncOutputIterator(self)

    def checkpoint(self) -> None:
        """协作式检查取消、Graph timeout 和当前 Node timeout。

        Raises:
            ExecutionCancelledError: 已接受取消请求。
            ExecutionTimeoutError: 已超过 Graph 总时长。
            NodeTimeoutError: 已超过当前 firing 的 Node timeout。
        """

        self._checkpoint()

    @contextmanager
    def step(self, node_id: str) -> Iterator[None]:
        """记录一次 Node firing，并在其完整生命周期内应用控制限制。

        进入时检查控制条件、计一步并开始 Node 计时；退出时再次检查并清理当前节点。

        Args:
            node_id: 本次 firing 的节点 ID，必须属于 start 时绑定的 Graph。

        Yields:
            None。

        Raises:
            ValueError: node_id 为空或不属于绑定的 Graph。
            RuntimeError: execution 不处于 RUNNING。
            StepLimitExceededError: 已达到 max_steps。
            ExecutionControlError: 进入或退出时检测到取消或超时。
        """

        node_id = require_non_empty_string(node_id, "execution node")
        with self._condition:
            try:
                timeout = (
                    None
                    if self._node_timeouts is None
                    else self._node_timeouts[node_id]
                )
            except KeyError as exc:
                raise ValueError(
                    f"node {node_id!r} does not belong to this execution"
                ) from exc
        self._begin_step(node_id, timeout)
        try:
            yield
        finally:
            self._end_step()

    def start(self, graph: Graph) -> bool:
        """执行宿主绑定冻结 Graph 的 Node timeout 并进入 RUNNING。

        Args:
            graph: 本次执行的冻结 Graph。

        Returns:
            进入 RUNNING 返回 True；启动前已取消时返回 False。

        Raises:
            TypeError: graph 不是冻结的 Graph。
            RuntimeError: 已经开始，或已绑定不同的 Graph 定义。
        """

        if not isinstance(graph, Graph) or not graph.frozen:
            raise TypeError("execution requires a frozen Graph")
        self._bind_node_timeouts(graph._execution_timeouts())
        with self._condition:
            if self._status is ExecutionStatus.CANCELLED:
                return False
            if self._status is not ExecutionStatus.PENDING:
                raise RuntimeError(f"execution {self.id!r} has already started")
            now = datetime.now(timezone.utc)
            self._status = ExecutionStatus.RUNNING
            self._started_at = now
            self._started_monotonic = time.monotonic()
            self._notifier.notify()
            return True

    def _bind_node_timeouts(
        self,
        timeouts: Mapping[str, float | None],
    ) -> None:
        """绑定 Graph 冻结时的 Node timeout 快照；重复绑定必须一致。

        Raises:
            RuntimeError: 已有执行步骤，或快照与已绑定的不同。
        """

        normalized: dict[str, float | None] = {}
        for raw_id, timeout in timeouts.items():
            node_id = require_non_empty_string(raw_id, "execution node")
            _validate_duration(timeout, f"node {node_id!r} timeout")
            normalized[node_id] = timeout
        with self._condition:
            if self._steps:
                raise RuntimeError("cannot bind node timeouts after execution steps")
            if self._node_timeouts is None:
                self._node_timeouts = normalized
            elif self._node_timeouts != normalized:
                raise RuntimeError("execution is bound to a different Graph definition")

    def _begin_step(self, node_id: str, timeout: float | None) -> None:
        """检查控制条件和步数上限，计一步并开始 Node 计时。

        Raises:
            RuntimeError: execution 不处于 RUNNING。
            StepLimitExceededError: 已达到 max_steps。
        """

        self._checkpoint()
        with self._condition:
            if self._status is not ExecutionStatus.RUNNING:
                raise RuntimeError("execution step requires RUNNING status")
            if self.limits.max_steps and self._steps >= self.limits.max_steps:
                raise StepLimitExceededError(
                    f"execution exceeded max_steps={self.limits.max_steps}",
                    graph=self.graph,
                    node=node_id,
                )
            self._steps += 1
            self._current_node = node_id
            self._node_started_monotonic = time.monotonic()
            self._step_timeout = timeout

    def _end_step(self) -> None:
        """检查 firing 结束时的控制条件，并始终清理当前节点计时。"""

        try:
            self._checkpoint()
        finally:
            with self._condition:
                self._current_node = None
                self._node_started_monotonic = None
                self._step_timeout = None

    def _checkpoint(self) -> None:
        """抛出取消异常，或抛出最先到期的 Graph/Node 超时异常。"""

        with self._condition:
            if self._cancel_requested:
                raise ExecutionCancelledError(
                    f"execution {self.id!r} was cancelled",
                    graph=self.graph,
                    node=self._current_node,
                )
            now = time.monotonic()
            expired: list[tuple[float, BaseException]] = []
            if self.limits.timeout is not None and self._started_monotonic is not None:
                deadline = self._started_monotonic + self.limits.timeout
                if now >= deadline:
                    expired.append(
                        (
                            deadline,
                            ExecutionTimeoutError(
                                f"execution exceeded timeout={self.limits.timeout}",
                                graph=self.graph,
                                node=self._current_node,
                            ),
                        )
                    )
            if (
                self._step_timeout is not None
                and self._node_started_monotonic is not None
            ):
                deadline = self._node_started_monotonic + self._step_timeout
                if now >= deadline:
                    expired.append(
                        (
                            deadline,
                            NodeTimeoutError(
                                f"node {self._current_node!r} exceeded "
                                f"timeout={self._step_timeout}",
                                graph=self.graph,
                                node=self._current_node,
                            ),
                        )
                    )
            if expired:
                raise min(expired, key=lambda item: item[0])[1]

    def wait_timeout(self) -> float | None:
        """先执行 `checkpoint()`，再返回距最近的 Graph 或 Node deadline 的秒数。

        Returns:
            剩余秒数（不小于 0，且不超过 threading.TIMEOUT_MAX，调用方循环重算）；
            没有生效的时限时返回 None。

        Raises:
            ExecutionControlError: 同 `checkpoint()`。
        """

        self._checkpoint()
        with self._condition:
            now = time.monotonic()
            remaining: list[float] = []
            if self.limits.timeout is not None and self._started_monotonic is not None:
                remaining.append(self.limits.timeout - (now - self._started_monotonic))
            if (
                self._step_timeout is not None
                and self._node_started_monotonic is not None
            ):
                remaining.append(
                    self._step_timeout - (now - self._node_started_monotonic)
                )
            # 超大有限时限会让线程等待溢出；截断后等待方到期醒来重新计算，语义不变。
            return None if not remaining else min(max(0.0, min(remaining)), TIMEOUT_MAX)

    def succeed(self) -> None:
        """执行宿主在执行器完成后标记成功；结果来自已发布的输出。

        已处于终态时不做任何事。

        Raises:
            RuntimeError: execution 尚未开始。
            ExecutionControlError: 结束前检测到取消或超时。
        """

        with self._condition:
            if self._status.terminal:
                return
            self.checkpoint()
            if self._status is not ExecutionStatus.RUNNING:
                raise RuntimeError("execution must be running before success")
            self._finish_locked(ExecutionStatus.SUCCEEDED)

    def fail(self, error: BaseException) -> None:
        """执行宿主记录失败；已发布输出仍可由流消费者读取。

        终态由 `terminal_status(error)` 决定；执行已结束时忽略本次调用。

        Args:
            error: 终止本次执行的异常，等待方会重新抛出它。

        Raises:
            TypeError: error 不是 BaseException 实例。
        """
        if not isinstance(error, BaseException):
            raise TypeError("execution failure must be a BaseException")
        with self._condition:
            if self._status.terminal:
                return
            self._finish_locked(terminal_status(error), error=error)

    def _finish_locked(
        self,
        status: ExecutionStatus,
        *,
        error: BaseException | None = None,
    ) -> None:
        """在持有 `_condition` 时写入终态、保存原始 traceback 并通知等待方。"""

        self._status = status
        self._error = error
        self._error_traceback = None if error is None else error.__traceback__
        self._current_node = None
        self._node_started_monotonic = None
        self._step_timeout = None
        self._finished_at = datetime.now(timezone.utc)
        self._notifier.notify()

    def _raise_error_locked(self) -> NoReturn:
        """从原始 traceback 重新抛出 `_error`，避免多次等待累积 traceback 帧。"""

        assert self._error is not None
        raise self._error.with_traceback(self._error_traceback)

    def _add_cancel_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        """登记取消被接受时同步调用的内部回调，返回幂等注销函数。

        回调在 `cancel()` 的调用线程、释放内部锁后执行，必须快速且不抛错；
        订阅前已接受的取消不会补发，调用方应在订阅后再检查 `checkpoint()`。
        """

        token = object()
        with self._condition:
            self._cancel_listeners[token] = listener

        def remove() -> None:
            """注销本次订阅；重复调用无作用。"""

            with self._condition:
                self._cancel_listeners.pop(token, None)

        return remove

    def _try_publish(self, output: Output) -> bool:
        """在背压允许时追加一项输出。

        Returns:
            已追加返回 True；有活跃订阅的未读输出达到 output_buffer 时返回 False。

        Raises:
            TypeError: output 不是 Output。
            RuntimeError: execution 不处于 RUNNING。
            ExecutionControlError: 检测到取消或超时。
        """

        if not isinstance(output, Output):
            raise TypeError("execution can publish only Output")
        with self._condition:
            self.checkpoint()
            if self._status is not ExecutionStatus.RUNNING:
                raise RuntimeError("output publication requires RUNNING status")
            # 订阅对象的 __del__ 可能在同线程内重入并删除游标，必须遍历快照。
            cursors = tuple(self._stream_cursors.values())
            if (
                not self._unbounded
                and cursors
                and len(self._store) - min(cursors) >= self.output_buffer
            ):
                return False
            self._store.append(output)
            self._notifier.notify()
            return True

    def publish_output(self, output: Output) -> None:
        """同步发布一项 terminal Output，背压时阻塞等待消费者。

        等待期间仍受取消、Graph timeout 和当前 Node timeout 约束。

        Args:
            output: 需要追加到输出存储的 terminal Output。

        Raises:
            TypeError: output 不是 Output。
            RuntimeError: execution 不处于 RUNNING。
            ExecutionControlError: 发布或等待期间检测到取消或超时。
        """

        while True:
            with self._condition:
                version = self._notifier.version
                if self._try_publish(output):
                    return
                timeout = self.wait_timeout()
            self._notifier.wait(version, timeout)

    async def apublish_output(self, output: Output) -> None:
        """异步发布一项 terminal Output，背压等待时不阻塞事件循环。

        控制约束与 `publish_output()` 相同，供异步 GraphExecutor 使用。

        Args:
            output: 需要追加到输出存储的 terminal Output。

        Raises:
            TypeError: output 不是 Output。
            RuntimeError: execution 不处于 RUNNING。
            ExecutionControlError: 发布或等待期间检测到取消或超时。
        """

        while True:
            with self._condition:
                version = self._notifier.version
                if self._try_publish(output):
                    return
                timeout = self.wait_timeout()
            try:
                await asyncio.wait_for(self._notifier.wait_async(version), timeout)
            except asyncio.TimeoutError:
                self.checkpoint()

    def _release_backpressure(self) -> None:
        """供执行宿主关闭时调用：解除订阅背压，让阻塞的发布方继续并把剩余输出全部留在存储中。

        订阅仍可从原游标完整读取；该操作不可撤销，对已结束的执行没有作用。
        """

        with self._condition:
            if self._unbounded:
                return
            self._unbounded = True
            self._notifier.notify()

    async def _await_result(self) -> tuple[Output, ...]:
        """异步等待终态并返回 `result(0)`；等待被取消时同时取消 execution。"""

        try:
            while True:
                with self._condition:
                    version = self._notifier.version
                    if self._status.terminal:
                        return self.result(0)
                await self._notifier.wait_async(version)
        except asyncio.CancelledError:
            self.cancel()
            raise

    def _attach_stream(self) -> int:
        """登记一个从索引 0 开始的输出游标并返回其编号。"""

        stream_id = next(self._stream_counter)
        with self._condition:
            self._stream_cursors[stream_id] = 0
            self._notifier.notify()
        return stream_id

    def _next_output(self, stream_id: int) -> Output | None:
        """阻塞读取游标处的下一项输出；成功结束且读完时返回 None。"""

        while True:
            with self._condition:
                version = self._notifier.version
                output, pending = self._poll_output(stream_id)
                if output is not None or not pending:
                    return output
            self._notifier.wait(version)

    async def _next_output_async(self, stream_id: int) -> Output | None:
        """异步读取游标处的下一项输出；成功结束且读完时返回 None。"""

        while True:
            with self._condition:
                version = self._notifier.version
                output, pending = self._poll_output(stream_id)
                if output is not None or not pending:
                    return output
            await self._notifier.wait_async(version)

    def _poll_output(self, stream_id: int) -> tuple[Output | None, bool]:
        """非阻塞读取游标处的输出。

        Returns:
            `(output, True)` 表示读到一项并前移游标；`(None, True)` 表示需要等待；
            `(None, False)` 表示执行成功结束且已读完，或订阅已被其他线程或任务关闭。

        Raises:
            BaseException: 执行失败且已读完已发布输出时的执行异常。
        """

        with self._condition:
            cursor = self._stream_cursors.get(stream_id)
            if cursor is None:
                # 阻塞读取期间订阅被并发关闭：按已结束处理，读取方正常停止迭代。
                return None, False
            if cursor < len(self._store):
                output = self._store[cursor]
                self._stream_cursors[stream_id] = cursor + 1
                self._notifier.notify()
                return output, True
            if self._status.terminal:
                if self._error is not None:
                    self._raise_error_locked()
                return None, False
            return None, True

    def _detach_stream(self, stream_id: int) -> None:
        """移除输出游标并唤醒因背压等待的生产者；游标不存在时只发送通知。"""

        with self._condition:
            self._stream_cursors.pop(stream_id, None)
            self._notifier.notify()
