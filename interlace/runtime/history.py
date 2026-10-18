"""GraphWorker 的本地 Execution 记录与空闲统计。"""

from __future__ import annotations

import time
from collections import OrderedDict
from itertools import islice
from threading import Condition

from ..engine.errors import InterlaceRuntimeError
from ..engine.execution import Execution


class _ExecutionHistory:
    """按执行 ID 保留 Execution，并统计宿主尚未收尾的执行。

    已收尾记录超过上限时从最早登记的一项开始淘汰；未收尾记录始终保留。同一 ID 的旧记录已收尾时，
    新记录替换它并移到最新位置。

    Attributes:
        _limit: 保留记录数量上限，至少为 1。
        _records: 按登记顺序保存的执行 ID 到 Execution 映射。
        _active: 已登记但执行宿主尚未收尾的执行，按 ID 索引。
        _condition: 保护记录，并在活跃执行归零时唤醒空闲等待方。
    """

    def __init__(self, limit: int) -> None:
        """创建空记录表。

        Args:
            limit: 保留记录数量上限，必须是不小于 1 的整数。

        Raises:
            TypeError: limit 不是整数。
            ValueError: limit 小于 1。
        """

        if type(limit) is not int:
            raise TypeError("history_limit must be an integer")
        if limit < 1:
            raise ValueError("history_limit must be at least 1")
        self._limit = limit
        self._records: OrderedDict[str, Execution] = OrderedDict()
        self._active: dict[str, Execution] = {}
        self._condition = Condition()

    @property
    def idle(self) -> bool:
        """返回是否没有尚未收尾的执行。"""

        with self._condition:
            return not self._active

    def record(self, execution: Execution) -> None:
        """登记新执行并将其计为活跃。

        Raises:
            InterlaceRuntimeError: 同一 ID 的前一次执行尚未收尾。
        """

        with self._condition:
            if execution.id in self._active:
                raise InterlaceRuntimeError(
                    f"execution {execution.id!r} is still active"
                )
            self._records.pop(execution.id, None)
            self._records[execution.id] = execution
            self._active[execution.id] = execution
            self._trim_locked()

    def finish(self, execution: Execution) -> None:
        """在执行宿主完成全部收尾后结束活跃计数。"""

        with self._condition:
            if self._active.get(execution.id) is not execution:
                return
            del self._active[execution.id]
            self._trim_locked()
            if not self._active:
                self._condition.notify_all()

    def get(self, execution_id: str) -> Execution:
        """按 ID 读取保留的执行。

        Raises:
            InterlaceRuntimeError: 没有保留该 ID 的执行。
        """

        with self._condition:
            try:
                return self._records[execution_id]
            except KeyError as exc:
                raise InterlaceRuntimeError(
                    f"unknown execution {execution_id!r}"
                ) from exc

    def snapshot(self) -> tuple[Execution, ...]:
        """按登记顺序返回保留执行的快照。"""

        with self._condition:
            return tuple(self._records.values())

    def wait_idle(self, timeout: float | None) -> bool:
        """等待活跃执行归零。

        Args:
            timeout: 最长等待秒数，None 表示不限时。

        Returns:
            活跃执行已经归零时为 True，超时为 False。
        """

        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._active:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def _trim_locked(self) -> None:
        """持锁按登记顺序淘汰超出上限的已收尾记录。"""

        excess = len(self._records) - self._limit
        if excess <= 0:
            return
        finished = list(
            islice(
                (key for key in self._records if key not in self._active),
                excess,
            )
        )
        for execution_id in finished:
            del self._records[execution_id]
