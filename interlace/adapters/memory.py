"""Runtime 的默认进程内 backend 实现。"""

from __future__ import annotations

import enum
import logging
import time
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from functools import partial
from threading import Condition, RLock

from .._validation import require_non_empty_string, validate_timeout
from ..engine.events import Event
from ..engine.slots import SlotPool
from ..spi import (
    Delivery,
    DeliveryOutcome,
    DeliveryResult,
    EventHandler,
    SlotLease,
    SlotProvider,
    Work,
    WorkHandler,
)

_logger = logging.getLogger(__name__)


class EventBus:
    """同步、进程内的 EventBus 默认实现。

    publish 在调用线程内依次调用选中的处理器；关闭是一次原子状态切换，不等待在途分发，并发调用安全。

    Attributes:
        _handlers: 事件类型 -> 订阅组 -> 注册身份 -> 处理器的三级映射。
        _anonymous: 生成独立匿名订阅组名称的递增计数。
        _next_handler: 各 (事件类型, 订阅组) 下一次轮询使用的处理器游标。
        _condition: 保护订阅表与分发计数，并唤醒 wait_idle 的条件变量。
        _active_dispatches: 尚未返回的 publish 调用数量。
        _closed: 是否已拒绝新的订阅和发布。
    """

    def __init__(self) -> None:
        """创建独立订阅表、轮询游标和事件分发计数。"""

        self._handlers: dict[str, dict[str, dict[object, EventHandler]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        self._anonymous = 0
        self._next_handler: dict[tuple[str, str], int] = defaultdict(int)
        self._condition = Condition(RLock())
        self._active_dispatches = 0
        self._closed = False

    @property
    def idle(self) -> bool:
        """判断是否没有尚未返回的 publish 调用。

        Returns:
            没有在途分发时为 True。
        """

        with self._condition:
            return self._active_dispatches == 0

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        *,
        subscription: str | None = None,
    ) -> Callable[[], None]:
        """注册事件订阅，同名订阅组中的处理器轮流消费。

        Args:
            event_type: 精确事件类型，或匹配全部类型的 ``"*"``。
            handler: 在 publish 调用线程内接收 Event 的处理器。
            subscription: 竞争消费组名称，None 创建独立订阅。

        Returns:
            仅注销本次注册的幂等函数，不撤回已被选中的投递。

        Raises:
            RuntimeError: EventBus 已经关闭。
            TypeError: handler 不可调用。
            ValueError: event_type 或 subscription 为空字符串。
        """

        event_type = require_non_empty_string(
            event_type,
            "subscription event type",
        )
        if not callable(handler):
            raise TypeError("event handler must be callable")
        with self._condition:
            if self._closed:
                raise RuntimeError("event bus is closed")
            if subscription is None:
                self._anonymous += 1
                subscription = f"__anonymous__:{self._anonymous}"
            else:
                subscription = require_non_empty_string(
                    subscription, "event subscription"
                )
            registrations = self._handlers[event_type][subscription]
            registration = object()
            registrations[registration] = handler

        def unsubscribe() -> None:
            """移除本次注册，保留同一回调的其他独立注册。"""

            with self._condition:
                if registration not in registrations:
                    return
                del registrations[registration]
                if not registrations:
                    del self._handlers[event_type][subscription]
                    self._next_handler.pop((event_type, subscription), None)
                    if not self._handlers[event_type]:
                        del self._handlers[event_type]

        return unsubscribe

    def publish(self, event: Event) -> None:
        """在调用线程内把 Event 交给每个订阅组选中的处理器。

        单个处理器失败不阻断其他订阅组；全部调用结束后重新抛出首个失败。

        Args:
            event: 需要分发的 Event。

        Raises:
            RuntimeError: EventBus 已经关闭。
            TypeError: event 不是 Event。
            Exception: 首个失败处理器抛出的原始异常。
        """

        if not isinstance(event, Event):
            raise TypeError("event bus accepts only Event")
        with self._condition:
            if self._closed:
                raise RuntimeError("event bus is closed")
            handlers = self._select_handlers(event.type)
            if event.type != "*":
                handlers += self._select_handlers("*")
            self._active_dispatches += 1
        failure: Exception | None = None
        try:
            for handler in handlers:
                try:
                    handler(event)
                except Exception as exc:
                    # 一个观察者失败不应阻断同一 Event 的其他订阅者。
                    if failure is None:
                        failure = exc
            if failure is not None:
                raise failure
        finally:
            with self._condition:
                self._active_dispatches -= 1
                self._condition.notify_all()

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待在途 publish 调用全部返回。

        Args:
            timeout: 最长等待秒数；None 表示一直等待。

        Raises:
            TimeoutError: 时限内仍有在途分发。
        """

        validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._active_dispatches:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("event bus did not become idle")
                self._condition.wait(remaining)

    def close(self) -> None:
        """拒绝后续订阅和发布；已经开始的 publish 继续完成。"""

        with self._condition:
            self._closed = True

    def _select_handlers(self, event_type: str) -> tuple[EventHandler, ...]:
        """为每个订阅组轮询选择一个事件处理器并推进游标。

        Args:
            event_type: 需要匹配的订阅事件类型。

        Returns:
            每个订阅组本次选中的处理器，没有订阅时为空元组。
        """

        selected: list[EventHandler] = []
        for subscription, handlers in self._handlers.get(event_type, {}).items():
            key = (event_type, subscription)
            index = self._next_handler[key] % len(handlers)
            self._next_handler[key] += 1
            selected.append(tuple(handlers.values())[index])
        return tuple(selected)


class _State(enum.Enum):
    """TaskBackend 的生命周期阶段。

    Attributes:
        OPEN: 接受绑定和提交。
        CLOSING: 拒绝新绑定，仍接受已有消费者通道的提交，以便在途交付的续作排空。
        CLOSED: 拒绝全部绑定和提交，线程池与自建 Slot 池已经关闭。
    """

    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(slots=True, eq=False)
class _Consumer:
    """一次 bind 调用对应的消费者注册。

    Attributes:
        handler: 在线程池中处理 Delivery 的函数。
        concurrency: 本消费者同时执行的 Delivery 上限。
        executor: 本消费者独占、最多 concurrency 个线程的线程池。
        slots: 为根 Work 申请 Slot lease 的提供方。
        queue: 本次绑定所属的通道名称。
        active: 已分配给本消费者、尚未完成 lease 清理的交付数量。
        bound: 是否仍可被分配新交付；注销或绑定失败后为 False。
    """

    handler: WorkHandler
    concurrency: int
    executor: ThreadPoolExecutor
    slots: SlotProvider
    queue: str
    active: int = 0
    bound: bool = True


@dataclass(slots=True)
class _Channel:
    """命名消费通道的排队投递与轮询状态；只有存在消费者时才接受提交。

    Attributes:
        consumers: 按注册顺序排列的已绑定消费者。
        queued: 尚未分配给消费者的投递，延续 Slot 链的投递优先分配。
        next_consumer: 下一次消费者轮询的起始下标。
        active: 本通道已分配、尚未完成 lease 清理的交付数量，包含正在注销的消费者。
    """

    consumers: list[_Consumer]
    queued: deque[Delivery]
    next_consumer: int = 0
    active: int = 0


class TaskBackend:
    """使用内存队列和线程池执行 Work 的默认实现。

    提交要求目标通道已有绑定的消费者，从未绑定或已经注销的通道立即拒绝，避免 Work 无人处理。
    交付失败只保留首个待传播异常供 wait_idle 抛出；保留期间出现的其他失败在记录时写入
    ``interlace.adapters.memory`` 的 ERROR 日志，不在内存中累积。

    Attributes:
        _max_delivery_attempts: 每个 Work 的最大交付次数，包含首次交付。
        _channels: 通道名称到通道状态的映射，首次 bind 时创建。
        _pending: 已提交到线程池、尚未完成回调的交付句柄。
        _failure: 等待 wait_idle 或 close 抛出的首个失败，None 表示没有。
        _condition: 保护全部可变状态，并唤醒空闲、注销和关闭等待方的条件变量。
        _slot_subscriptions: SlotProvider 身份到 (提供方, 可用通知注销函数) 的映射。
        _owned_slot_pools: bind 未传入 slots 时自建、由本实例关闭的 SlotPool。
        _next_channel: 下一次跨通道轮询的起始下标。
        _state: 当前生命周期阶段。
    """

    def __init__(self, *, max_delivery_attempts: int = 3) -> None:
        """创建空通道表，并校验最大交付次数。

        Args:
            max_delivery_attempts: 每个 Work 的最大交付次数，包含首次交付。

        Raises:
            ValueError: max_delivery_attempts 不是正整数。
        """

        if type(max_delivery_attempts) is not int or max_delivery_attempts < 1:
            raise ValueError(
                "max_delivery_attempts must be an integer greater than zero"
            )
        self._max_delivery_attempts = max_delivery_attempts
        self._channels: dict[str, _Channel] = {}
        self._pending: set[Future[DeliveryResult]] = set()
        self._failure: BaseException | None = None
        self._condition = Condition(RLock())
        self._slot_subscriptions: dict[
            int, tuple[SlotProvider, Callable[[], None]]
        ] = {}
        self._owned_slot_pools: list[SlotPool] = []
        self._next_channel = 0
        self._state = _State.OPEN

    @property
    def idle(self) -> bool:
        """判断是否没有排队或执行中的交付。

        Returns:
            所有通道都没有排队投递且线程池中没有在途交付时为 True。
        """

        with self._condition:
            return not self._pending and not any(
                channel.queued for channel in self._channels.values()
            )

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
        slots: SlotProvider | None = None,
    ) -> Callable[[], None]:
        """为命名通道注册一个拥有独立线程池的消费者。

        Args:
            queue: 命名消费通道。
            handler: 在线程池中处理 Delivery 并返回 DeliveryResult 的函数。
            concurrency: 本消费者同时执行的 Delivery 上限。
            slots: 为根 Work 申请 Slot 的提供方；None 时自建容量为 concurrency 的 SlotPool。

        Returns:
            注销本次绑定的幂等函数；最后一个消费者先排空通道，再停止接收，重新绑定后恢复。

        Raises:
            RuntimeError: TaskBackend 正在关闭或已经关闭。
            TypeError: handler 不可调用、concurrency 不是整数或 slots 不实现 SlotProvider。
            ValueError: queue 为空字符串或 concurrency 小于 1。
        """

        queue = require_non_empty_string(queue, "task queue")
        if not callable(handler):
            raise TypeError("work handler must be callable")
        if type(concurrency) is not int:
            raise TypeError("queue concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("queue concurrency must be at least 1")
        if slots is not None and not isinstance(slots, SlotProvider):
            raise TypeError("slots must implement SlotProvider or be None")
        failure: BaseException | None = None
        with self._condition:
            if self._state is not _State.OPEN:
                raise RuntimeError("task backend is closed")
            if slots is None:
                slots = SlotPool(concurrency)
                owned_slots = slots
            else:
                owned_slots = None
            executor = ThreadPoolExecutor(
                max_workers=concurrency,
                thread_name_prefix=f"interlace-{queue}",
            )
            consumer = _Consumer(handler, concurrency, executor, slots, queue)
            try:
                self._subscribe_slots(slots)
            except BaseException:
                executor.shutdown(wait=True)
                if owned_slots is not None:
                    owned_slots.close()
                raise
            if owned_slots is not None:
                self._owned_slot_pools.append(owned_slots)
            channel = self._channels.setdefault(queue, _Channel([], deque()))
            channel.consumers.append(consumer)
            try:
                self._drain_channel(channel)
            except BaseException as exc:
                # 已分配的交付可以完成，但失败绑定不得再接收后续工作。
                channel.consumers.remove(consumer)
                consumer.bound = False
                failure = exc
            self._condition.notify_all()

        if failure is not None:
            try:
                self._unbind(consumer)
            finally:
                raise failure
        return partial(self._unbind, consumer)

    def _unbind(self, consumer: _Consumer) -> None:
        """注销一个消费者：最后消费者先排空通道，再等待在途清理并释放专属资源。"""

        with self._condition:
            if consumer.bound:
                channel = self._channels[consumer.queue]
                while (
                    consumer.bound
                    and len(channel.consumers) == 1
                    and (channel.active or channel.queued)
                ):
                    self._condition.wait()
                if consumer.bound:
                    consumer.bound = False
                    channel.consumers.remove(consumer)
            while consumer.active:
                self._condition.wait()
        consumer.executor.shutdown(wait=True)
        with self._condition:
            if not any(
                other.slots is consumer.slots
                for channel in self._channels.values()
                for other in channel.consumers
            ):
                subscription = self._slot_subscriptions.pop(id(consumer.slots), None)
                if subscription is not None:
                    subscription[1]()
            if consumer.slots in self._owned_slot_pools:
                self._owned_slot_pools.remove(consumer.slots)
                consumer.slots.close()
            self._condition.notify_all()

    def submit(self, queue: str, work: Work) -> None:
        """向已有消费者的通道提交根 Work，由消费者为其申请新的 Slot。

        Args:
            queue: 已绑定消费者的通道名称。
            work: 需要执行的 Work。

        Raises:
            RuntimeError: TaskBackend 已经关闭，或通道从未绑定、已注销全部消费者。
            TypeError: work 不是 Work。
            ValueError: queue 为空字符串。
        """

        queue = require_non_empty_string(queue, "task queue")
        if not isinstance(work, Work):
            raise TypeError("task backend accepts only Work")
        self._enqueue(queue, Delivery(work))

    def submit_local(self, queue: str, work: Work, lease: SlotLease) -> None:
        """提交延续当前进程 Slot 链的 Work；正常返回时接管调用方转交的一个 lease 引用。

        Args:
            queue: 已绑定消费者的通道名称。
            work: 需要执行的 Work。
            lease: 随本次投递传递的 Slot 引用，抛错时仍由调用方释放。

        Raises:
            RuntimeError: TaskBackend 已经关闭，或通道从未绑定、已注销全部消费者。
            TypeError: work 不是 Work 或 lease 不实现 SlotLease。
            ValueError: queue 为空字符串。
        """

        queue = require_non_empty_string(queue, "task queue")
        if not isinstance(work, Work):
            raise TypeError("task backend accepts only Work")
        self._enqueue(queue, Delivery(work, slot_lease=lease))

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待全部排队和在途交付完成，然后抛出并清除首个待传播失败。

        同一等待周期内的其他失败已在发生时写入日志，不会再次抛出。

        Args:
            timeout: 最长等待秒数；None 表示一直等待。

        Raises:
            TimeoutError: 时限内仍有排队或在途交付。
            BaseException: 交付期间记录的首个失败。
        """

        validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            self._wait_until_idle(deadline)
            failure = self._take_failure()
        if failure is not None:
            raise failure

    def close(self) -> None:
        """排空已接受交付后关闭全部消费者线程池和自建 Slot 池。

        CLOSING 期间拒绝新绑定，但仍接受已有消费者通道的提交以完成续作；排空后在同一临界区内撤下全部
        消费者，此后提交被拒绝。并发调用 close 的其他线程等待首个调用完成后返回。

        Raises:
            BaseException: 排空期间记录的首个失败，或关闭资源时的首个失败。
        """

        with self._condition:
            if self._state is not _State.OPEN:
                while self._state is not _State.CLOSED:
                    self._condition.wait()
                return
            self._state = _State.CLOSING
        failure: BaseException | None = None
        try:
            with self._condition:
                self._wait_until_idle(None)
                failure = self._take_failure()
                consumers = tuple(
                    consumer
                    for channel in self._channels.values()
                    for consumer in channel.consumers
                )
                for consumer in consumers:
                    consumer.bound = False
                for channel in self._channels.values():
                    channel.consumers.clear()
                subscriptions = tuple(self._slot_subscriptions.values())
                self._slot_subscriptions.clear()
                pools = tuple(reversed(self._owned_slot_pools))
                self._owned_slot_pools.clear()
            for consumer in consumers:
                consumer.executor.shutdown(wait=True)
            for release in (
                *(unsubscribe for _, unsubscribe in subscriptions),
                *(pool.close for pool in pools),
            ):
                try:
                    release()
                except Exception as exc:
                    if failure is None:
                        failure = exc
        finally:
            with self._condition:
                self._state = _State.CLOSED
                self._condition.notify_all()
        if failure is not None:
            raise failure

    def _enqueue(self, queue: str, delivery: Delivery) -> None:
        """把投递放入已有消费者的通道并尝试立即分配；没有消费者或已关闭时拒绝。"""

        with self._condition:
            if self._state is _State.CLOSED:
                raise RuntimeError("task backend is closed")
            channel = self._channels.get(queue)
            if channel is None or not channel.consumers:
                if self._state is _State.CLOSING:
                    raise RuntimeError("task backend is closed")
                raise RuntimeError(f"task queue {queue!r} has no active consumers")
            channel.queued.append(delivery)
            self._drain_safely(channel)

    def _wait_until_idle(self, deadline: float | None) -> None:
        """在已持有条件锁时等待空闲；deadline 为单调时钟截止点。

        Raises:
            TimeoutError: 截止点前仍有排队或在途交付。
        """

        while not self.idle:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError("task backend did not become idle")
            self._condition.wait(remaining)

    def _take_failure(self) -> BaseException | None:
        """取出并清除首个待传播失败。"""

        failure, self._failure = self._failure, None
        return failure

    def _record_failure(self, failure: BaseException) -> None:
        """保留首个待传播失败；已有待传播失败时写入 ERROR 日志，避免静默丢弃。"""

        if self._failure is None:
            self._failure = failure
        else:
            _logger.error(
                "task backend failure not propagated; an earlier failure is pending",
                exc_info=failure,
            )

    def _drain_safely(self, channel: _Channel | None = None) -> None:
        """在已持有条件锁时推进指定或全部通道，推进失败记为待传播失败，并总是唤醒等待方。"""

        try:
            if channel is None:
                self._drain_all()
            else:
                self._drain_channel(channel)
        except BaseException as exc:
            self._record_failure(exc)
        finally:
            self._condition.notify_all()

    def _done(
        self,
        consumer: _Consumer,
        delivery: Delivery,
        future: Future[DeliveryResult],
    ) -> None:
        """线程池完成回调：结算交付结果，释放交付持有的 lease，再推进等待通道。"""

        with self._condition:
            try:
                failure = self._settle(consumer, delivery, future)
            except BaseException as exc:
                failure = exc
            if failure is not None:
                self._record_failure(failure)
        try:
            if delivery.slot_lease is not None:
                delivery.slot_lease.release()
        except BaseException as exc:
            with self._condition:
                self._record_failure(exc)
        finally:
            with self._condition:
                self._pending.discard(future)
                consumer.active -= 1
                self._channels[consumer.queue].active -= 1
                self._drain_safely()

    def _settle(
        self,
        consumer: _Consumer,
        delivery: Delivery,
        future: Future[DeliveryResult],
    ) -> BaseException | None:
        """按交付结果决定是否重新排队，在已持有条件锁时调用。

        RETRY 未耗尽次数且通道仍有消费者时，为重投递额外保留一个 lease 引用并放回队首。

        Returns:
            需要记录的失败；ACK、无错误的 REJECT 和已重新排队的 RETRY 返回 None。
        """

        failure = future.exception()
        if failure is not None:
            return failure
        result = future.result()
        if not isinstance(result, DeliveryResult):
            return TypeError("work handler must return DeliveryResult")
        if result.outcome is DeliveryOutcome.REJECT:
            return result.error
        if result.outcome is not DeliveryOutcome.RETRY:
            return None
        channel = self._channels[consumer.queue]
        if delivery.attempt >= self._max_delivery_attempts:
            return result.error or RuntimeError(
                f"work {delivery.work.id!r} exhausted "
                f"max_delivery_attempts={self._max_delivery_attempts}"
            )
        if not channel.consumers:
            return result.error or RuntimeError(
                f"task queue {consumer.queue!r} has no active consumers"
            )
        if delivery.slot_lease is not None:
            delivery.slot_lease.retain()
        channel.queued.appendleft(
            Delivery(
                delivery.work,
                delivery.attempt + 1,
                slot_lease=delivery.slot_lease,
            )
        )
        return None

    def _dispatch(self, consumer: _Consumer, delivery: Delivery) -> None:
        """把已取得 Slot 的投递交给消费者线程池；提交失败时记录失败并释放 lease。"""

        consumer.active += 1
        self._channels[consumer.queue].active += 1
        try:
            future = consumer.executor.submit(consumer.handler, delivery)
        except BaseException as exc:
            consumer.active -= 1
            self._channels[consumer.queue].active -= 1
            self._record_failure(exc)
            if delivery.slot_lease is not None:
                try:
                    delivery.slot_lease.release()
                except BaseException as release_error:
                    self._record_failure(release_error)
            self._condition.notify_all()
            return
        self._pending.add(future)
        future.add_done_callback(partial(self._done, consumer, delivery))

    def _drain_all(self) -> None:
        """从轮询游标开始依次推进每个通道，并把游标移到下一个通道。"""

        channels = tuple(self._channels.values())
        if not channels:
            return
        start = self._next_channel % len(channels)
        for offset in range(len(channels)):
            self._drain_channel(channels[(start + offset) % len(channels)])
        self._next_channel = (start + 1) % len(channels)

    def _drain_channel(self, channel: _Channel) -> None:
        """优先分配携带 Slot 的延续投递，再为队首根投递申请空闲 Slot。"""

        while channel.queued and channel.consumers:
            available = self._available_consumers(channel)
            if not available:
                return
            continuation = next(
                (
                    index
                    for index, delivery in enumerate(channel.queued)
                    if delivery.slot_lease is not None
                ),
                None,
            )
            if continuation is not None:
                delivery = channel.queued[continuation]
                del channel.queued[continuation]
                consumer = available[0]
                self._advance_consumer(channel, consumer)
                self._dispatch(consumer, delivery)
                continue

            delivery = channel.queued[0]
            assigned: tuple[_Consumer, Delivery] | None = None
            for consumer in available:
                lease = consumer.slots.try_acquire()
                if lease is not None:
                    assigned = (consumer, replace(delivery, slot_lease=lease))
                    break
            if assigned is None:
                return
            channel.queued.popleft()
            consumer, delivery = assigned
            self._advance_consumer(channel, consumer)
            self._dispatch(consumer, delivery)

    @staticmethod
    def _advance_consumer(channel: _Channel, consumer: _Consumer) -> None:
        """把通道轮询游标移到刚分配的消费者之后。"""

        index = channel.consumers.index(consumer)
        channel.next_consumer = index + 1

    @staticmethod
    def _available_consumers(channel: _Channel) -> list[_Consumer]:
        """按轮询顺序返回尚未达到本地并发上限的消费者。"""

        count = len(channel.consumers)
        start = channel.next_consumer % count
        ordered = channel.consumers[start:] + channel.consumers[:start]
        return [
            consumer for consumer in ordered if consumer.active < consumer.concurrency
        ]

    def _subscribe_slots(self, slots: SlotProvider) -> None:
        """为每个 SlotProvider 只订阅一次可用通知，通知到达时推进等待通道。"""

        key = id(slots)
        if key in self._slot_subscriptions:
            return

        def available() -> None:
            """Slot 归还后推进等待通道；推进失败记为待传播失败，不传回 SlotPool。"""

            with self._condition:
                if self._state is not _State.CLOSED:
                    self._drain_safely()

        self._slot_subscriptions[key] = (slots, slots.subscribe_available(available))
