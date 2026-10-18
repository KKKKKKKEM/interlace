"""Runtime 角色共享的私有生命周期辅助函数。"""

from __future__ import annotations

import time
from enum import Enum
from threading import Condition, get_ident
from typing import Any

from ..engine.errors import RuntimeClosedError


class _LifecycleState(Enum):
    """Runtime 组件的关闭阶段。"""

    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


class _Lifecycle:
    """线程安全的 OPEN -> CLOSING -> CLOSED 关闭状态机。

    首个 close 调用独占排空与释放过程；并发调用等待同一次关闭完成，关闭线程内的重入调用立即返回。

    Attributes:
        _name: 错误消息中使用的组件名称。
        _state: 当前关闭阶段。
        _closer: 执行关闭的线程标识，未进入关闭时为 None。
        _condition: 保护状态并唤醒关闭等待者的条件变量。
    """

    def __init__(self, name: str) -> None:
        """创建处于 OPEN 状态的生命周期。"""

        self._name = name
        self._state = _LifecycleState.OPEN
        self._closer: int | None = None
        self._condition = Condition()

    @property
    def state(self) -> _LifecycleState:
        """返回当前关闭阶段。"""

        with self._condition:
            return self._state

    def ensure_open(self) -> None:
        """拒绝在关闭开始后接受新的根工作。

        Raises:
            RuntimeClosedError: 组件处于 CLOSING 或 CLOSED。
        """

        with self._condition:
            if self._state is not _LifecycleState.OPEN:
                raise RuntimeClosedError(f"{self._name} is closed")

    def begin_close(self) -> bool:
        """原子进入 CLOSING，或等待其他线程完成的同一次关闭。

        Returns:
            当前调用负责执行关闭时为 True；已关闭、其他线程完成关闭或关闭线程重入时为 False。
        """

        with self._condition:
            if self._state is _LifecycleState.OPEN:
                self._state = _LifecycleState.CLOSING
                self._closer = get_ident()
                return True
            if self._closer != get_ident():
                while self._state is not _LifecycleState.CLOSED:
                    self._condition.wait()
            return False

    def finish_close(self) -> None:
        """进入 CLOSED，并唤醒等待同一次关闭的调用方。"""

        with self._condition:
            self._state = _LifecycleState.CLOSED
            self._closer = None
            self._condition.notify_all()


def _unique(*components: object) -> tuple[object, ...]:
    """按对象身份去重组件，保留首次出现顺序。"""

    unique: list[object] = []
    for component in components:
        if all(component is not item for item in unique):
            unique.append(component)
    return tuple(unique)


def _close_components(
    components: Any,
    failure: BaseException | None,
) -> BaseException | None:
    """依次关闭去重后的组件，保留最先出现的失败。

    控制异常同样只记录不中断，保证后续组件仍被关闭，最后由调用方重新抛出。

    Args:
        components: 需要依次关闭的组件集合。
        failure: 先前已经记录的失败，None 表示没有失败。

    Returns:
        最先出现的异常；全部关闭成功且没有先前失败时为 None。
    """

    for component in components:
        try:
            component.close()
        except BaseException as exc:
            if failure is None:
                failure = exc
    return failure


def _remaining(deadline: float | None) -> float | None:
    """根据单调时钟截止点计算剩余等待秒数。

    Args:
        deadline: 单调时钟上的截止时间，None 表示不限时。

    Returns:
        距离截止点的非负秒数；未设置截止点时返回 None。
    """

    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())
