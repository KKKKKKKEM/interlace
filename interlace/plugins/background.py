"""把 Notice 处理函数移出调用路径的有界后台投递。"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from queue import Full, Queue
from threading import Lock, Thread
from typing import Any, Generic, TypeVar

T = TypeVar("T")

_logger = logging.getLogger(__name__)
_STOP = object()


class Background(Generic[T]):
    """在专属后台线程中执行同步 Notice 处理函数的适配器。

    适配器本身是普通的 Notice 处理函数：触发方只把值放入有界队列，不等待处理；队列已满或适配器已关闭时
    丢弃该值并计数，从不阻塞触发方。处理函数按入队顺序在单个守护线程中依次执行，失败写入
    ``interlace.plugins.background`` 的 ERROR 日志后继续处理后续值。值以原对象投递，处理函数不得修改它。

    插件以 ``api.on(point, adapter)`` 登记、以 ``api.defer(adapter.close)`` 在卸载或关闭时排空::

        observer = Background(export)
        api.defer(observer.close)
        api.on(RUNTIME_EVENT, observer)

    Attributes:
        _handler: 被移出调用路径的同步处理函数。
        _queue: 待处理值的有界队列；关闭时在末尾追加停止标记。
        _lock: 保护关闭状态、丢弃计数与入队。
        _closed: 是否已关闭；关闭后新值全部丢弃。
        _dropped: 因队列已满或已关闭而丢弃的值数量。
        _thread: 依次执行处理函数的守护线程。
    """

    def __init__(self, handler: Callable[[T], object], *, maxsize: int = 1024) -> None:
        """保存处理函数并启动后台线程。

        Args:
            handler: 同步处理函数，签名同 Notice 处理函数。
            maxsize: 队列最多容纳的待处理值数量。

        Raises:
            TypeError: handler 不可调用、是协程函数，或 maxsize 不是 int。
            ValueError: maxsize 小于 1。
        """

        if not callable(handler):
            raise TypeError("background handler must be callable")
        if inspect.iscoroutinefunction(handler):
            raise TypeError("background handler must be synchronous")
        if isinstance(maxsize, bool) or not isinstance(maxsize, int):
            raise TypeError("maxsize must be an int")
        if maxsize < 1:
            raise ValueError("maxsize must be positive")
        self._handler = handler
        self._queue: Queue[Any] = Queue(maxsize)
        self._lock = Lock()
        self._closed = False
        self._dropped = 0
        self._thread = Thread(
            target=self._run, name="interlace-background-notice", daemon=True
        )
        self._thread.start()

    @property
    def dropped(self) -> int:
        """返回因队列已满或已关闭而丢弃的值数量。"""

        with self._lock:
            return self._dropped

    def __call__(self, value: T) -> None:
        """把值放入队列；队列已满或已关闭时丢弃并计数。

        Args:
            value: Notice 点交付的只读值。
        """

        with self._lock:
            if not self._closed:
                try:
                    self._queue.put_nowait(value)
                    return
                except Full:
                    pass
            self._dropped += 1

    def close(self) -> None:
        """停止接收新值，并等待已入队的值全部处理完毕；重复调用只等待同一次排空。"""

        with self._lock:
            first = not self._closed
            self._closed = True
        if first:
            # 关闭后不再有新值入队，阻塞放入停止标记只等待后台线程腾出位置。
            self._queue.put(_STOP)
        self._thread.join()

    def _run(self) -> None:
        """按入队顺序执行处理函数，直到取到停止标记。"""

        while (value := self._queue.get()) is not _STOP:
            try:
                result = self._handler(value)
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise TypeError("background handler returned an awaitable")
            except Exception:
                _logger.exception("background notice handler failed")
