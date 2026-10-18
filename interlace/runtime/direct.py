"""GraphWorker 直接执行使用的线程池。"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from ..engine.errors import RuntimeClosedError


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
