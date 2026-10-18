"""可跨 Work 传递并长期复用状态的逻辑执行槽。"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from contextlib import AbstractContextManager, contextmanager
from threading import Condition, Lock, RLock
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from .._validation import require_non_empty_string, validate_timeout

_LOGGER = logging.getLogger(__name__)


@runtime_checkable
class SlotLease(Protocol):
    """适配器持有的进程内 Slot 能力，由 SlotPool 分配并从 interlace.spi 导出。"""

    @property
    def slot(self) -> Slot:
        """当前执行链使用的 Slot；读取不转移引用所有权。

        Raises:
            RuntimeError: 全部引用已释放。
        """
        ...

    def retain(self) -> None:
        """为分支或后续投递增加一个引用，必须匹配一次 release。

        Raises:
            RuntimeError: 全部引用已释放。
        """

    def release(self) -> None:
        """释放一个引用；最后一个引用归还 Slot，归还后不可再次使用。

        Raises:
            RuntimeError: 全部引用已释放。
        """

    def execution(self) -> AbstractContextManager[Slot]:
        """串行占用 Slot 执行一次 Graph execution。

        进入时保留一个临时引用，再无超时地阻塞等待同一 Slot 上的其他 execution 退出；
        退出时只释放该临时引用，不释放调用方持有的引用。同一线程嵌套进入同一 Slot
        会死锁，同一逻辑链的后续 Work 必须在当前 execution 结束后执行。

        Returns:
            产出当前 Slot 的上下文管理器。
        """
        ...


@runtime_checkable
class SlotProvider(Protocol):
    """Consumer 使用的本地 Slot 池能力，不要求继承默认 SlotPool。"""

    @property
    def size(self) -> int:
        """池的固定容量。"""
        ...

    @property
    def available(self) -> int:
        """当前未租出的 Slot 数量。"""
        ...

    def try_acquire(self) -> SlotLease | None:
        """立即取得一个根执行链 lease；没有空闲 Slot 时返回 None。

        Returns:
            持有一个引用的 lease，调用方负责释放；池耗尽时为 None。
        """
        ...

    def acquire(self, timeout: float | None = None) -> SlotLease:
        """阻塞等待一个根执行链 lease。

        Args:
            timeout: 最长等待秒数；None 表示一直等待，0 表示不等待。

        Returns:
            持有一个引用的 lease，调用方负责释放。
        """
        ...

    def subscribe_available(self, listener: Callable[[], None]) -> Callable[[], None]:
        """订阅 Slot 归还通知；通知只提示重试申请，不预留 Slot。

        Args:
            listener: Slot 归还后调用的无参函数。

        Returns:
            只注销本次订阅的幂等函数。
        """
        ...

    def close(self) -> None:
        """停止新的申请并唤醒等待方；已租出的 lease 仍可稍后归还。"""
        ...


class Slot(MutableMapping[str, Any]):
    """保存一条逻辑执行链可复用的资源，与线程和 Worker 无关。

    Slot 按身份比较与哈希：内容相同的两个 Slot 仍是不同资源，可作为字典键或集合成员。

    Attributes:
        id: Slot 标识，缺省为随机 UUID 字符串；同一池内唯一。
        _values: 按名称保存的资源，由 Slot 独占持有。
        _execution_lock: 串行化同一 Slot 上 Graph execution 的非重入锁，只由 lease 使用。
    """

    __eq__ = object.__eq__
    __hash__ = object.__hash__

    def __init__(
        self,
        values: Mapping[str, Any] | None = None,
        *,
        id: str | None = None,
    ) -> None:
        """创建 Slot，并复制初始资源映射。

        Args:
            values: 初始资源；只复制顶层映射，不复制资源对象。
            id: Slot 标识；None 时生成随机 UUID。

        Raises:
            ValueError: id 为空字符串。
        """

        self.id = require_non_empty_string(
            str(uuid4()) if id is None else id,
            "slot id",
        )
        self._values: dict[str, Any] = {} if values is None else dict(values)
        self._execution_lock = Lock()

    def __getitem__(self, key: str) -> Any:
        """按名称读取资源。

        Raises:
            KeyError: 名称不存在。
        """

        return self._values[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """按名称保存资源，供同一逻辑执行链的后续 Work 使用。"""

        self._values[key] = value

    def __delitem__(self, key: str) -> None:
        """移除指定名称的资源。

        Raises:
            KeyError: 名称不存在。
        """

        del self._values[key]

    def __iter__(self) -> Iterator[str]:
        """迭代资源名称。"""

        return iter(self._values)

    def __len__(self) -> int:
        """返回资源数量。"""

        return len(self._values)

    def __repr__(self) -> str:
        """返回只包含 id 的调试表示，不展开资源内容。"""

        return f"Slot(id={self.id!r})"


class SlotPool:
    """管理一组同构 Slot；同一个池可以被多个 Consumer 共享。

    Attributes:
        _slots: 池拥有的全部 Slot，构造后不变。
        _available: 尚未租出的 Slot，按归还顺序先进先出。
        _condition: 保护可用队列、订阅与关闭状态，并唤醒阻塞的 acquire。
        _listeners: 订阅令牌到归还通知函数的映射，按订阅顺序调用。
        _closed: 是否已关闭；关闭后拒绝申请与订阅。
    """

    def __init__(
        self,
        size: int | None = None,
        *,
        slots: Iterable[Slot] | None = None,
        factory: Callable[[], Slot] | None = None,
    ) -> None:
        """用固定数量的新 Slot 或调用方提供的 Slot 创建池。

        Args:
            size: 新建 Slot 的数量，至少为 1；与 slots 互斥。
            slots: 直接托管的 Slot，至少一个且实例与 id 均不重复。
            factory: 创建单个 Slot 的无参函数，默认 `Slot`；只能与 size 一起使用。

        Raises:
            TypeError: 参数组合互斥、size 不是 int、factory 不可调用或元素不是 Slot。
            ValueError: size 小于 1、slots 为空，或包含重复实例或重复 id。
        """

        if slots is not None and size is not None:
            raise TypeError("size and slots are mutually exclusive")
        if slots is not None and factory is not None:
            raise TypeError("factory cannot be combined with slots")
        if factory is not None and not callable(factory):
            raise TypeError("slot factory must be callable")
        if slots is None:
            if type(size) is not int:
                raise TypeError("slot pool size must be an integer")
            if size < 1:
                raise ValueError("slot pool size must be at least 1")
            if factory is None:
                factory = Slot
            created = tuple(factory() for _ in range(size))
        else:
            created = tuple(slots)
            if not created:
                raise ValueError("slot pool must contain at least one Slot")
        if any(not isinstance(slot, Slot) for slot in created):
            raise TypeError("slot pool accepts only Slot instances")
        if len({id(slot) for slot in created}) != len(created):
            raise ValueError("slot pool must not contain duplicate Slot instances")
        if len({slot.id for slot in created}) != len(created):
            raise ValueError("slot pool must not contain duplicate Slot ids")

        self._slots = created
        self._available = deque(created)
        self._condition = Condition(RLock())
        self._listeners: dict[object, Callable[[], None]] = {}
        self._closed = False

    @property
    def size(self) -> int:
        """池的固定容量。"""

        return len(self._slots)

    @property
    def available(self) -> int:
        """当前未租出的 Slot 数量。"""

        with self._condition:
            return len(self._available)

    def close(self) -> None:
        """关闭池并唤醒等待方；在途 lease 仍可稍后归还，重复调用无作用。"""

        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._condition.notify_all()

    def try_acquire(self) -> SlotLease | None:
        """立即申请一个根执行链 lease。

        Returns:
            持有一个引用的 lease；池耗尽时返回 None。

        Raises:
            RuntimeError: 池已关闭。
        """

        with self._condition:
            if self._closed:
                raise RuntimeError("slot pool is closed")
            if not self._available:
                return None
            return _SlotLease(self, self._available.popleft())

    def subscribe_available(self, listener: Callable[[], None]) -> Callable[[], None]:
        """订阅 Slot 归还通知；通知不预留 Slot。

        每次订阅独立登记：同一函数订阅多次会收到多次通知，各自的注销函数只移除对应
        那一次订阅。通知在归还 Slot 的线程上、释放池锁后调用，抛出的 Exception 被记录。

        Args:
            listener: Slot 归还后调用的无参函数。

        Returns:
            只注销本次订阅的幂等函数。

        Raises:
            TypeError: listener 不可调用。
            RuntimeError: 池已关闭。
        """

        if not callable(listener):
            raise TypeError("slot pool listener must be callable")
        token = object()
        with self._condition:
            if self._closed:
                raise RuntimeError("slot pool is closed")
            self._listeners[token] = listener

        def unsubscribe() -> None:
            """移除本次订阅；重复调用无作用。"""

            with self._condition:
                self._listeners.pop(token, None)

        return unsubscribe

    def acquire(self, timeout: float | None = None) -> SlotLease:
        """阻塞等待一个根执行链 lease；不得在 Consumer 执行线程内等待。

        Args:
            timeout: 最长等待秒数；None 表示一直等待，0 表示不等待。

        Returns:
            持有一个引用的 lease，调用方负责释放。

        Raises:
            RuntimeError: 池已关闭或在等待期间关闭。
            TimeoutError: timeout 内没有可用 Slot。
        """

        validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._available:
                if self._closed:
                    raise RuntimeError("slot pool is closed")
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("slot pool did not provide a Slot")
                self._condition.wait(remaining)
            if self._closed:
                raise RuntimeError("slot pool is closed")
            return _SlotLease(self, self._available.popleft())

    def _release(self, slot: Slot) -> None:
        """归还 Slot，唤醒一个阻塞的 acquire，并在锁外调用全部归还订阅。

        Raises:
            ValueError: Slot 不属于本池。
            RuntimeError: Slot 已在可用队列中。
        """

        with self._condition:
            if all(slot is not item for item in self._slots):
                raise ValueError("Slot does not belong to this pool")
            if any(slot is item for item in self._available):
                raise RuntimeError("Slot is already available")
            self._available.append(slot)
            self._condition.notify()
            listeners = tuple(self._listeners.values())
        for listener in listeners:
            try:
                listener()
            except Exception:
                _LOGGER.exception("slot availability listener failed")


class _SlotLease:
    """SlotPool 的默认 lease：对同一执行链的 Slot 做引用计数，最后一个引用归还。

    Attributes:
        __slots__: 固定实例字段。
        _slot: 本 lease 租用的 Slot。
        _pool: 最后一个引用释放时归还的池。
        _references: 尚未释放的引用数量；为 0 后 lease 失效。
        _lock: 保护引用计数的锁。
    """

    __slots__ = ("_lock", "_pool", "_references", "_slot")

    def __init__(self, pool: SlotPool, slot: Slot) -> None:
        """创建持有一个引用的 lease。"""

        self._slot = slot
        self._pool = pool
        self._references = 1
        self._lock = RLock()

    @property
    def slot(self) -> Slot:
        """本 lease 租用的 Slot。

        Raises:
            RuntimeError: 全部引用已释放。
        """

        with self._lock:
            if self._references == 0:
                raise RuntimeError("slot lease is already released")
            return self._slot

    @contextmanager
    def execution(self) -> Iterator[Slot]:
        """保留临时引用并独占 Slot 的执行锁，语义见 `SlotLease.execution`。

        Yields:
            本 lease 租用的 Slot。

        Raises:
            RuntimeError: 全部引用已释放。
        """

        self.retain()
        try:
            with self._slot._execution_lock:
                yield self._slot
        finally:
            self.release()

    def retain(self) -> None:
        """增加一个必须由接管方释放的引用。

        Raises:
            RuntimeError: 全部引用已释放。
        """

        with self._lock:
            if self._references == 0:
                raise RuntimeError("slot lease is already released")
            self._references += 1

    def release(self) -> None:
        """释放一个引用；最后一个引用释放后把 Slot 归还给池。

        Raises:
            RuntimeError: 全部引用已释放。
        """

        slot: Slot | None = None
        with self._lock:
            if self._references == 0:
                raise RuntimeError("slot lease is already released")
            self._references -= 1
            if self._references == 0:
                slot = self._slot
        if slot is not None:
            self._pool._release(slot)
