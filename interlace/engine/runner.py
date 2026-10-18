"""在同步 Engine 边界内执行同步和异步调用。"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import logging
from threading import Event as ThreadEvent
from threading import RLock, Thread, current_thread
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .execution import Execution

_LOGGER = logging.getLogger(__name__)

# close() 等待后台 loop 线程退出的最长秒数；超过后记录警告并放弃等待。
_CLOSE_JOIN_TIMEOUT = 5.0


class LocalRunner:
    """同步返回普通值，并在私有后台 event loop 线程上等待 awaitable。

    异步 Node 与 Hook 的协程运行在本 Runner 独占的 loop 线程上，而不是调用方自己的
    event loop；同一 Graph execution 的调用串行推进，因此 loop 绑定的资源（client、
    锁、Future 等）必须在节点协程内部创建，不得跨 loop 复用。取消通过向协程任务发送
    CancelledError 实现，抑制 CancelledError 的协程会阻塞取消和关闭。

    Attributes:
        _loop: 后台线程独占运行的 event loop。
        _ready: 后台线程已设置当前 loop 并即将进入 run_forever 的信号。
        _lock: 保护 _closed 与任务提交顺序的锁。
        _closed: 是否已调用 close；关闭后拒绝新的 awaitable。
        _thread: 运行 _loop 的守护线程。
    """

    def __init__(self) -> None:
        """启动后台 loop 线程，并等待它就绪后返回。"""

        self._loop = asyncio.new_event_loop()
        self._ready = ThreadEvent()
        self._lock = RLock()
        self._closed = False
        self._thread = Thread(
            target=self._run_loop,
            name="interlace-async-runner",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def resolve(self, value: Any, *, execution: Execution | None = None) -> Any:
        """原样返回非 awaitable 值，或阻塞等待后台 loop 上的 awaitable 结果。

        提供 execution 时，等待由任务完成或取消请求直接唤醒，不轮询；Graph 与 Node
        deadline 到期同样唤醒。控制条件触发后先取消协程任务，并等待它完成清理
        （包括 finally）再重新抛出控制异常。

        Args:
            value: 普通返回值或需要等待的 awaitable。
            execution: 施加取消与超时约束的执行句柄；None 时无约束地等待结果。

        Returns:
            value 本身，或 awaitable 的结果。

        Raises:
            RuntimeError: Runner 已关闭，或在 Runner 自身的 loop 线程上同步等待。
            ExecutionControlError: 等待期间检测到取消、Graph timeout 或 Node timeout。
            BaseException: awaitable 自身抛出的异常。
        """

        if not inspect.isawaitable(value):
            return value
        future, scheduled = self._submit(value)
        if execution is None:
            return future.result()
        wake = ThreadEvent()
        future.add_done_callback(lambda _: wake.set())
        remove_listener = execution._add_cancel_listener(wake.set)
        try:
            while True:
                # wait_timeout 先执行 checkpoint，取消或超时时直接抛出。
                remaining = execution.wait_timeout()
                if future.done():
                    return future.result()
                wake.wait(remaining)
        except BaseException:
            # 代理 Future 可能先于协程清理完成；必须等实际任务退出才释放执行资源。
            if not future.done():
                task = scheduled.result()
                self._loop.call_soon_threadsafe(task.cancel)
                try:
                    future.result()
                except (concurrent.futures.CancelledError, asyncio.CancelledError):
                    pass
                except BaseException:
                    # 清理失败不能替换原控制异常，但必须留下可见记录。
                    _LOGGER.exception("awaitable cleanup failed after cancellation")
            raise
        finally:
            remove_listener()

    def _submit(
        self, value: Any
    ) -> tuple[
        concurrent.futures.Future[Any],
        concurrent.futures.Future[asyncio.Future[Any]],
    ]:
        """把 awaitable 提交到后台 loop。

        Returns:
            `(result, task)`：result 转交任务结果或异常；task 在调度成功后给出 loop 内任务，
            供取消使用。

        Raises:
            RuntimeError: Runner 已关闭，或调用方位于 Runner 自身的 loop 线程。
        """

        with self._lock:
            if self._closed:
                if inspect.iscoroutine(value):
                    value.close()
                raise RuntimeError("runner is closed")
            if current_thread() is self._thread:
                if inspect.iscoroutine(value):
                    value.close()
                raise RuntimeError(
                    "cannot synchronously resolve an awaitable on the runner thread"
                )
            future: concurrent.futures.Future[Any] = concurrent.futures.Future()
            scheduled: concurrent.futures.Future[asyncio.Future[Any]] = (
                concurrent.futures.Future()
            )

            def completed(task: asyncio.Future[Any]) -> None:
                """把 loop 内任务的结果或异常转交给同步等待方。"""

                try:
                    future.set_result(task.result())
                except BaseException as exc:
                    future.set_exception(exc)

            def schedule() -> None:
                """在 loop 线程内创建任务；创建失败时同时结束两个 Future。"""

                try:
                    task = asyncio.ensure_future(value, loop=self._loop)
                    task.add_done_callback(completed)
                    scheduled.set_result(task)
                except BaseException as exc:
                    scheduled.set_exception(exc)
                    future.set_exception(exc)

            self._loop.call_soon_threadsafe(schedule)
        return future, scheduled

    def close(self) -> None:
        """停止后台 loop，取消残留任务并有界等待线程退出；重复调用无作用。

        残留任务收到 CancelledError 并完成清理后 loop 才关闭。若协程抑制取消或清理
        超过 `_CLOSE_JOIN_TIMEOUT` 秒，close 记录警告后返回，守护线程继续运行到任务结束。
        """

        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(_CLOSE_JOIN_TIMEOUT)
        if self._thread.is_alive():
            _LOGGER.warning(
                "async runner thread did not exit within %.1f seconds; "
                "a coroutine may be suppressing cancellation",
                _CLOSE_JOIN_TIMEOUT,
            )

    def _run_loop(self) -> None:
        """运行后台 loop；停止后取消并等待残留任务，再关闭 loop。"""

        asyncio.set_event_loop(self._loop)
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self._loop.close()
