"""GraphWorker 直接执行使用的线程池与不占线程的 Slot 等待。"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from ..engine.errors import RuntimeClosedError
from ..engine.execution import Execution
from ..engine.slots import SlotLease, SlotProvider


class _DirectRunner:
    """在独立线程池中运行 `start/iter/aiter` 直接提交的 Graph execution。

    Attributes:
        _pool: 承载直接执行的线程池。
        _lock: 串行提交与关闭，保证关闭后不再接受任务。
        _closed: 是否已经停止接受新任务。
    """

    def __init__(self, max_workers: int | None) -> None:
        """创建线程池。

        Args:
            max_workers: 线程数上限；None 使用标准库 ThreadPoolExecutor 的默认值。

        Raises:
            TypeError: max_workers 既不是整数也不是 None。
            ValueError: max_workers 小于 1。
        """

        if max_workers is not None:
            if type(max_workers) is not int:
                raise TypeError("direct_workers must be an integer or None")
            if max_workers < 1:
                raise ValueError("direct_workers must be at least 1")
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="interlace-direct",
        )
        self._lock = Lock()
        self._closed = False

    def submit(self, task: Callable[[], None]) -> None:
        """提交任务；任务自行处理异常与收尾。

        Raises:
            RuntimeClosedError: 线程池已经关闭。
        """

        with self._lock:
            if self._closed:
                raise RuntimeClosedError("GraphWorker is closed")
            self._pool.submit(task)

    def close(self) -> None:
        """拒绝新任务，并等待已提交任务全部结束。"""

        with self._lock:
            self._closed = True
        self._pool.shutdown(wait=True)


class _SlotWait:
    """为一次 PENDING 直接执行申请 SlotLease，等待期间不占用任何线程。

    开始等待时立即申请一次，此后只在 SlotProvider 的归还或关闭通知中重试。取得 lease、Execution 被取消或
    申请失败三者只结束一次：取得时把 lease 交给 acquired，否则调用 abandoned。结束时先注销可用通知与
    取消监听；取消恰好晚于取得 lease 时由执行宿主在 `start()` 返回 False 后释放 lease，不会泄漏。

    Attributes:
        _execution: 等待 Slot 的直接执行句柄，只读取其终态与取消通知。
        _slots: 申请根执行链 lease 的提供方。
        _acquired: 取得 lease 后在锁外调用的回调，接管 lease 的引用。
        _abandoned: 取消或申请失败后在锁外调用的回调；参数为申请异常，取消时为 None。
        _lock: 串行化申请、取消与收尾，保证等待只结束一次且同一时刻最多一个申请在途。
        _done: 等待是否已结束。
        _detach: 尚未调用的可用通知注销函数与取消监听注销函数。
    """

    __slots__ = (
        "_abandoned",
        "_acquired",
        "_detach",
        "_done",
        "_execution",
        "_lock",
        "_slots",
    )

    def __init__(
        self,
        execution: Execution,
        slots: SlotProvider,
        acquired: Callable[[SlotLease], None],
        abandoned: Callable[[BaseException | None], None],
    ) -> None:
        """保存等待目标与收尾回调，不开始等待。"""

        self._execution = execution
        self._slots = slots
        self._acquired = acquired
        self._abandoned = abandoned
        self._lock = Lock()
        self._done = False
        self._detach: list[Callable[[], None]] = []

    def begin(self) -> None:
        """登记取消监听与可用通知，然后立即申请一次。

        订阅失败（包括 SlotProviderClosedError）按申请失败结束等待。

        Raises:
            BaseException: acquired 回调在本线程取得 lease 时抛出的失败。
        """

        self._attach(self._execution._add_cancel_listener(self._cancel))
        try:
            unsubscribe = self._slots.subscribe_available(self._attempt)
        except BaseException as exc:
            self._abandon(exc)
            return
        self._attach(unsubscribe)
        self._attempt()

    def _attach(self, detach: Callable[[], None]) -> None:
        """保存注销函数；等待已经结束时立即调用它。"""

        with self._lock:
            if not self._done:
                self._detach.append(detach)
                return
        detach()

    def _attempt(self) -> None:
        """申请一次 lease；Execution 已结束时按取消收尾，没有空闲 Slot 时继续等待。

        Raises:
            BaseException: acquired 回调的失败。
        """

        with self._lock:
            if self._done:
                return
            lease: SlotLease | None = None
            error: BaseException | None = None
            if not self._execution.done:
                try:
                    lease = self._slots.try_acquire()
                except BaseException as exc:
                    error = exc
                else:
                    if lease is None:
                        return
            detach = self._finish_locked()
        _call_all(detach)
        if lease is None:
            self._abandoned(error)
        else:
            self._acquired(lease)

    def _cancel(self) -> None:
        """取消监听：Execution 已由 cancel() 写入 CANCELLED，结束等待。"""

        self._abandon(None)

    def _abandon(self, error: BaseException | None) -> None:
        """以取消（error 为 None）或申请失败结束等待；等待已结束时无作用。"""

        with self._lock:
            if self._done:
                return
            detach = self._finish_locked()
        _call_all(detach)
        self._abandoned(error)

    def _finish_locked(self) -> list[Callable[[], None]]:
        """持锁标记等待结束，并取出待调用的注销函数。"""

        self._done = True
        detach, self._detach = self._detach, []
        return detach


def _call_all(functions: list[Callable[[], None]]) -> None:
    """依次调用无参注销函数。"""

    for function in functions:
        function()
