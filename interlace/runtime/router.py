"""Event 发布、观察与跨图 Work 路由。"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextvars import ContextVar
from functools import partial
from threading import RLock
from types import TracebackType
from typing import Any

from ..adapters import memory
from .._validation import require_non_empty_string
from ..engine.errors import EventDispatchError, InterlaceRuntimeError
from ..engine.events import Event
from ..engine.execution import ExecutionLimits
from ..engine.extension import HookBus, Registration
from ..engine.observation import RuntimeEvent, RuntimeEventKind
from ..engine.points import RUNTIME_EVENT
from ..spi import EventBus, LocalTaskPublisher, SlotLease, TaskPublisher, Work
from .points import EVENT_EMIT, WORK_ROUTE
from ._utils import _close_components, _Lifecycle, _unique

EventHandler = Callable[[Event], None]
# 当前同步分发所属的源 Slot lease；只由 route handler 的 Work 提交读取，观察者调用期间清空。
_LOCAL_LEASE: ContextVar[SlotLease | None] = ContextVar(
    "interlace_local_event_lease",
    default=None,
)
_logger = logging.getLogger(__name__)


class EventRouter:
    """发布和订阅 Event，并把匹配的 Event 转成队列 Work。

    分发前经 EVENT_EMIT 转换或拒绝事件，提交前经 WORK_ROUTE 转换或拒绝 Work；route handler 与钩子的失败以
    EventDispatchError 传回 emit 调用方；observe 注册的观察者异常写入日志后隔离，不改变 emit 结果。
    关闭时先进入 CLOSING 拒绝新的注册与发布，再注销订阅、等待在途分发并关闭拥有的组件。

    Attributes:
        _events: 承载订阅与分发的 EventBus。
        _publisher: 接收路由 Work 的任务发布端。
        _owned_components: 由当前 Router 自建并负责关闭的组件。
        _close_injected: 是否在关闭时一并关闭注入的 EventBus 与任务发布端。
        _hooks: 所属 Runtime 的钩子总线，发布 EVENT_PUBLISHED 与 WORK_SUBMITTED 并调用路由钩子点。
        _routes: 已注册的 (事件类型, Graph 名称, 队列) 路由，用于拒绝重复路由。
        _subscriptions: 当前 Router 在 EventBus 上的订阅注销函数，按注册顺序保存。
        _lock: 保护路由表和订阅列表的可重入锁。
        _lifecycle: OPEN -> CLOSING -> CLOSED 关闭状态机。
    """

    def __init__(
        self,
        *,
        publisher: TaskPublisher,
        events: EventBus | None = None,
        close_injected: bool = False,
        hooks: HookBus | None = None,
    ) -> None:
        """装配事件传输、任务发布和钩子总线，明确注入资源的所有权。

        Args:
            publisher: 接收路由 Work 的任务发布端；实现 LocalTaskPublisher 时可延续本地 Slot 链。
            events: 注入的 EventBus；None 时自建并负责关闭默认内存实现。
            close_injected: 是否在关闭时一并关闭注入的 events 与 publisher。
            hooks: 所属 Runtime 的钩子总线；None 时创建独立总线。

        Raises:
            TypeError: close_injected 不是布尔值，或 hooks 不是 HookBus。
        """

        if type(close_injected) is not bool:
            raise TypeError("close_injected must be a boolean")
        if hooks is None:
            hooks = HookBus()
        elif not isinstance(hooks, HookBus):
            raise TypeError("hooks must be a HookBus")
        owned: list[object] = []
        if events is None:
            events = memory.EventBus()
            owned.append(events)
        self._events = events
        self._publisher = publisher
        self._owned_components = owned
        self._close_injected = close_injected
        self._hooks = hooks
        self._routes: set[tuple[str, str, str]] = set()
        self._subscriptions: list[Callable[[], None]] = []
        self._lock = RLock()
        self._lifecycle = _Lifecycle("EventRouter")

    @property
    def idle(self) -> bool:
        """判断 EventBus 是否没有在途分发。

        Returns:
            EventBus 报告空闲时为 True。
        """

        return self._events.idle

    def observe(self, event_type: str, handler: EventHandler) -> Registration:
        """注册一个相互独立的 Event 观察者。

        观察者在发布线程内调用，调用期间不关联源 Slot lease；其普通异常写入日志后忽略，不传回 emit。

        Args:
            event_type: 精确事件类型，或匹配全部类型的 ``"*"``。
            handler: 接收 Event 的观察函数。

        Returns:
            撤销本次观察的句柄。

        Raises:
            RuntimeClosedError: Router 正在关闭或已经关闭。
            TypeError: handler 不可调用。
            ValueError: event_type 为空字符串。
        """

        self._lifecycle.ensure_open()
        event_type = require_non_empty_string(event_type, "subscription event type")
        if not callable(handler):
            raise TypeError("event handler must be callable")
        with self._lock:
            self._lifecycle.ensure_open()
            unsubscribe = self._events.subscribe(event_type, partial(_observe, handler))
            self._subscriptions.append(unsubscribe)
        return Registration(partial(self._unsubscribe, unsubscribe, None))

    def route(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        subscription: str | None = None,
        limits: ExecutionLimits | None = None,
    ) -> Registration:
        """订阅 Event，并为每次匹配向队列提交目标 Graph 的 Work。

        Args:
            event_type: 精确事件类型，或匹配全部类型的 ``"*"``。
            graph: 目标 Graph 的注册名称。
            queue: 接收 Work 的命名通道。
            subscription: 竞争消费组名称；None 时按事件类型、Graph 与队列生成。
            limits: 每个 Work 独立使用的步数、时长与队列限制；None 表示不限制。

        Returns:
            撤销本次路由的句柄；撤销后可以重新登记相同路由。

        Raises:
            InterlaceRuntimeError: 相同的事件类型、Graph 与队列路由已经存在。
            RuntimeClosedError: Router 正在关闭或已经关闭。
            TypeError: limits 不是 ExecutionLimits。
            ValueError: 任一名称为空字符串。
        """

        self._lifecycle.ensure_open()
        event_type = require_non_empty_string(event_type, "subscription event type")
        graph = require_non_empty_string(graph, "route graph")
        queue = require_non_empty_string(queue, "route queue")
        if limits is None:
            limits = ExecutionLimits()
        if not isinstance(limits, ExecutionLimits):
            raise TypeError("route limits must be ExecutionLimits or None")
        if subscription is None:
            subscription = f"route:{event_type}:{graph}:{queue}"
        else:
            subscription = require_non_empty_string(subscription, "route subscription")
        with self._lock:
            self._lifecycle.ensure_open()
            route = (event_type, graph, queue)
            if route in self._routes:
                raise InterlaceRuntimeError(f"duplicate event route {route!r}")
            unsubscribe = self._events.subscribe(
                event_type,
                partial(self._submit, graph, queue, limits),
                subscription=subscription,
            )
            self._subscriptions.append(unsubscribe)
            self._routes.add(route)
        return Registration(partial(self._unsubscribe, unsubscribe, route))

    def _unsubscribe(
        self, unsubscribe: Callable[[], None], route: tuple[str, str, str] | None
    ) -> None:
        """撤销一项订阅并移除对应路由记录；Router 关闭后订阅已统一注销。"""

        with self._lock:
            if unsubscribe not in self._subscriptions:
                return
            self._subscriptions.remove(unsubscribe)
            if route is not None:
                self._routes.discard(route)
        unsubscribe()

    def emit(self, event_or_type: Event | str, payload: Any = None) -> Event:
        """发布完整 Event，或从 type 和 payload 创建后发布。

        Args:
            event_or_type: 已有 Event，或用于构造 Event 的类型字符串。
            payload: 构造 Event 时携带的领域数据；传入完整 Event 时必须为 None。

        Returns:
            EventBus 已经接受的 Event；EVENT_EMIT 处理函数可能替换它。

        Raises:
            EventDispatchError: EVENT_EMIT、EventBus 或 route handler 失败。
            RuntimeClosedError: Router 正在关闭或已经关闭。
            TypeError: 完整 Event 与 payload 同时传入。
        """

        if isinstance(event_or_type, Event):
            if payload is not None:
                raise TypeError("complete Event must not be combined with payload")
            event = event_or_type
        else:
            event = Event(event_or_type, payload)
        return self._dispatch(event, None)

    def publish(self, event: Event) -> Event:
        """发布 Event，路由产生的 Work 开始新的 Slot 链；供 GraphWorker 的 emit 回调使用。

        Args:
            event: 需要发布的 Event。

        Returns:
            经 EVENT_EMIT 转换后实际分发的 Event。

        Raises:
            EventDispatchError: EVENT_EMIT、EventBus 或 route handler 失败。
            RuntimeClosedError: Router 正在关闭或已经关闭。
        """

        return self._dispatch(event, None)

    def publish_local(self, event: Event, lease: SlotLease) -> Event:
        """发布 Event，让本次同步分发中的路由 Work 延续 lease 所属的 Slot 链。

        只有 route handler 的 Work 提交读取 lease；观察者及其嵌套发布不继承该 lease。

        Args:
            event: 需要发布的 Event。
            lease: 源 execution 持有的 Slot lease；每个本地路由分支各自 retain 一个引用。

        Returns:
            经 EVENT_EMIT 转换后实际分发的 Event。

        Raises:
            EventDispatchError: EVENT_EMIT、EventBus 或 route handler 失败。
            RuntimeClosedError: Router 正在关闭或已经关闭。
        """

        return self._dispatch(event, lease)

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待 EventBus 的在途分发完成。

        Args:
            timeout: 最长等待秒数；None 表示一直等待。

        Raises:
            TimeoutError: 时限内仍有在途分发。
        """

        self._events.wait_idle(timeout)

    def close(self) -> None:
        """注销订阅、等待在途分发，并关闭当前 Router 拥有的组件。

        并发调用等待首个调用完成后返回；关闭线程内的重入调用立即返回。

        Raises:
            Exception: 注销、等待或关闭组件时出现的首个失败。
        """

        if not self._lifecycle.begin_close():
            return
        failure: BaseException | None = None
        try:
            with self._lock:
                subscriptions = tuple(reversed(self._subscriptions))
                self._subscriptions.clear()
            for unsubscribe in subscriptions:
                try:
                    unsubscribe()
                except BaseException as exc:
                    if failure is None:
                        failure = exc
            try:
                self.wait_idle()
            except BaseException as exc:
                if failure is None:
                    failure = exc
            components = (
                _unique(self._events, self._publisher)
                if self._close_injected
                else tuple(self._owned_components)
            )
            failure = _close_components(reversed(components), failure)
        finally:
            self._lifecycle.finish_close()
        if failure is not None:
            raise failure

    def __enter__(self) -> EventRouter:
        """进入资源作用域并返回当前 Router。

        Returns:
            当前 Router。

        Raises:
            RuntimeClosedError: Router 正在关闭或已经关闭。
        """

        self._lifecycle.ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """退出资源作用域并关闭 Router；已有异常时不以关闭失败覆盖它。"""

        del exc_type, traceback
        try:
            self.close()
        except Exception:
            if exc_value is None:
                raise

    def _dispatch(self, event: Event, lease: SlotLease | None) -> Event:
        """经 EVENT_EMIT 中间件包裹：记录 EVENT_PUBLISHED，再在关联 lease 的上下文中交给 EventBus 分发。

        Returns:
            中间件返回的 Event；未改写时为实际分发的 Event。

        Raises:
            EventDispatchError: EVENT_EMIT 中间件、EventBus 或 route handler 抛出普通异常，
                或中间件返回的不是 Event。
            RuntimeClosedError: Router 正在关闭或已经关闭。
        """

        # 关闭先注销路由再排空，CLOSING 期间接受的分发将找不到路由，因此明确拒绝而非静默丢弃。
        self._lifecycle.ensure_open()

        def publish(current: Event) -> Event:
            """记录并分发中间件交来的 Event。"""

            # 先于分发记录，保证同一 Event 引起的 WORK_SUBMITTED 按因果顺序出现在其后。
            self._notify(
                RuntimeEvent(RuntimeEventKind.EVENT_PUBLISHED, event_type=current.type)
            )
            token = _LOCAL_LEASE.set(lease)
            try:
                self._events.publish(current)
            except EventDispatchError:
                raise
            except Exception as exc:
                raise EventDispatchError(current, exc) from exc
            finally:
                _LOCAL_LEASE.reset(token)
            return current

        try:
            dispatched = self._hooks.wrap(
                EVENT_EMIT, event, publish, event_type=event.type
            )
        except EventDispatchError:
            raise
        except Exception as exc:
            raise EventDispatchError(event, exc) from exc
        if not isinstance(dispatched, Event):
            error = TypeError(f"{EVENT_EMIT.id} middleware must return an Event")
            raise EventDispatchError(event, error) from error
        return dispatched

    def _notify(self, event: RuntimeEvent) -> None:
        """经 RUNTIME_EVENT 发布生命周期事实。"""

        self._hooks.notify(RUNTIME_EVENT, event, kind=event.kind, graph=event.graph)

    def _submit(
        self,
        graph: str,
        queue: str,
        limits: ExecutionLimits,
        event: Event,
    ) -> None:
        """route handler：把 Event 转成目标 Graph 的 Work，经 WORK_ROUTE 中间件包裹后提交到队列。

        存在本地 lease 且发布端实现 LocalTaskPublisher 时为本分支 retain 一个引用并转交；
        submit_local 抛错时由本函数释放该引用一次，释放失败写入日志，保留原始提交失败。
        其他情况提交不携带 lease 的根 Work。中间件不调用 call_next 时不提交。
        """

        def submit(work: Work) -> None:
            """提交中间件交来的 Work 并记录 WORK_SUBMITTED。"""

            lease = _LOCAL_LEASE.get()
            if lease is not None and isinstance(self._publisher, LocalTaskPublisher):
                lease.retain()
                try:
                    self._publisher.submit_local(queue, work, lease)
                except BaseException:
                    try:
                        lease.release()
                    except Exception:
                        _logger.exception(
                            "failed to release a rejected local Work lease"
                        )
                    raise
            else:
                self._publisher.submit(queue, work)
            self._notify(
                RuntimeEvent(
                    RuntimeEventKind.WORK_SUBMITTED,
                    graph=work.graph,
                    work_id=work.id,
                    event_type=event.type,
                    attributes={"queue": queue},
                )
            )

        self._hooks.wrap(
            WORK_ROUTE,
            Work(graph, event.payload, trigger=event, limits=limits),
            submit,
            event_type=event.type,
            graph=graph,
            queue=queue,
        )


def _observe(handler: EventHandler, event: Event) -> None:
    """在清空本地 lease 的上下文中调用观察者，记录并隔离其普通异常。"""

    token = _LOCAL_LEASE.set(None)
    try:
        handler(event)
    except Exception:
        _logger.exception("event observer failed for %r", event.type)
    finally:
        _LOCAL_LEASE.reset(token)
