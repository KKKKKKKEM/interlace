"""共享传输的注册生命周期、失败回滚和交付清理契约。"""

from contextlib import AbstractContextManager
from collections.abc import Callable
from threading import Event as ThreadEvent
from threading import Thread

import pytest

from interlace import Event, Graph, Node, Runtime, Slot, SlotPool
from interlace.adapters import memory
from interlace.runtime import EventRouter, GraphWorker, LocalRuntimePlugin
from interlace.spi import Delivery, DeliveryResult, SlotLease, Work, WorkHandler


def test_event_subscription_detaches_only_its_registration() -> None:
    """同一回调的每次订阅独立注销，且注销函数可以重复调用。"""

    bus = memory.EventBus()
    seen: list[Event] = []
    first = bus.subscribe("value", seen.append, subscription="shared")
    second = bus.subscribe("value", seen.append, subscription="shared")
    try:
        first()
        first()
        bus.publish(Event("value", 1))
        assert [event.payload for event in seen] == [1]
        second()
        bus.publish(Event("value", 2))
        assert [event.payload for event in seen] == [1]
    finally:
        bus.close()


def test_closed_roles_release_shared_transport_registrations() -> None:
    """关闭角色后旧订阅和消费者消失，共享组件支持重新装配。"""

    class Record(Node):
        """将执行次数写入调用方持有的记录。"""

        def execute(self, inputs, context) -> None:
            """记录一次执行。

            Args:
                inputs: 本次执行的入口数据。
                context: 当前节点上下文。
            """

            del inputs, context
            seen.append("execution")

    bus = memory.EventBus()
    tasks = memory.TaskBackend()
    seen: list[str] = []
    observed: list[Event] = []
    try:
        for _ in range(2):
            router = EventRouter(events=bus, publisher=tasks)
            worker = GraphWorker(consumer=tasks, emit=router.publish)
            with Runtime(router=router, worker=worker) as runtime:
                runtime.register(
                    "graph", Graph(entrypoint="record").add("record", Record())
                )
                runtime.on("value", graph="graph", queue="queue")
                runtime.observe("value", observed.append)
                runtime.emit("value")
                runtime.wait_idle(1)
            bus.publish(Event("value"))
            assert tasks.idle
        assert seen == ["execution", "execution"]
        assert len(observed) == 2
    finally:
        if tasks.idle:
            tasks.close()
        bus.close()


def test_failed_bind_can_be_retried_with_a_healthy_pool() -> None:
    """资源订阅失败不会留下消费者或阻断后续有效绑定。"""

    closed = SlotPool(1)
    closed.close()
    healthy = SlotPool(1)
    tasks = memory.TaskBackend()
    seen: list[str] = []

    def handle(delivery: Delivery) -> DeliveryResult:
        """记录交付并确认完成。

        Args:
            delivery: 本次交付。

        Returns:
            已完成交付的确认结果。
        """

        seen.append(delivery.work.graph)
        return DeliveryResult.ack()

    try:
        with pytest.raises(RuntimeError, match="closed"):
            tasks.bind("queue", handle, concurrency=1, slots=closed)
        tasks.bind("queue", handle, concurrency=1, slots=healthy)
        tasks.submit("queue", Work("graph"))
        tasks.wait_idle(1)
        assert seen == ["graph"]
    finally:
        if tasks.idle:
            tasks.close()
        healthy.close()


@pytest.mark.parametrize("failure_at", [1, 2])
def test_failed_initial_dispatch_rolls_back_binding(failure_at: int) -> None:
    """首次调度失败移除注册，并等待此前已经分配的交付完成清理。

    Args:
        failure_at: 资源申请失败的次数；2 表示已有一次交付在途。
    """

    acquisition_failed = ThreadEvent()
    handler_started = ThreadEvent()
    handler_allowed = ThreadEvent()
    binding_finished = ThreadEvent()

    class FailingPool(SlotPool):
        """在指定申请次数失败，并记录通知订阅的资源池。

        Attributes:
            acquisitions: 已尝试申请资源的次数。
            subscriptions: 当前仍然有效的可用通知订阅数量。
        """

        def __init__(self) -> None:
            """创建能同时承载两个交付的池。"""

            super().__init__(2)
            self.acquisitions = 0
            self.subscriptions = 0

        def try_acquire(self) -> SlotLease | None:
            """在指定次数报告申请失败。

            Returns:
                当前申请得到的资源引用，池耗尽时为 None。

            Raises:
                RuntimeError: 到达测试指定的失败次数。
            """

            self.acquisitions += 1
            if self.acquisitions == failure_at:
                acquisition_failed.set()
                raise RuntimeError("initial dispatch failed")
            return super().try_acquire()

        def subscribe_available(
            self, listener: Callable[[], None]
        ) -> Callable[[], None]:
            """记录本次订阅，并在注销时减少计数。

            Args:
                listener: 资源归还时的通知函数。

            Returns:
                注销本次通知的幂等函数。
            """

            detach = super().subscribe_available(listener)
            self.subscriptions += 1
            active = True

            def unsubscribe() -> None:
                """移除本次通知并更新有效订阅计数。"""

                nonlocal active
                if active:
                    active = False
                    detach()
                    self.subscriptions -= 1

            return unsubscribe

    stale_calls: list[str] = []
    healthy_calls: list[str] = []
    failures: list[BaseException] = []
    tasks = memory.TaskBackend()
    slots = FailingPool()

    def stale(delivery: Delivery) -> DeliveryResult:
        """阻塞已分配的交付，验证绑定回滚在锁外等待。

        Args:
            delivery: 初次调度已经分配的交付。

        Returns:
            已完成交付的确认结果。
        """

        stale_calls.append(delivery.work.graph)
        handler_started.set()
        assert handler_allowed.wait(2)
        return DeliveryResult.ack()

    def healthy(delivery: Delivery) -> DeliveryResult:
        """接管失败绑定未能分配的排队工作。

        Args:
            delivery: 留在后端队列的交付。

        Returns:
            已完成交付的确认结果。
        """

        healthy_calls.append(delivery.work.graph)
        return DeliveryResult.ack()

    def bind() -> None:
        """记录失败绑定的错误及其清理完成时机。"""

        try:
            tasks.bind("queue", stale, concurrency=2, slots=slots)
        except BaseException as exc:
            failures.append(exc)
        finally:
            binding_finished.set()

    tasks.submit("queue", Work("first"))
    if failure_at == 2:
        tasks.submit("queue", Work("second"))
    thread = Thread(target=bind)
    thread.start()
    try:
        assert acquisition_failed.wait(1)
        if failure_at == 2:
            assert handler_started.wait(1)
        returned_before_release = binding_finished.wait(0.03)
    finally:
        handler_allowed.set()
        thread.join(1)
    assert not thread.is_alive()
    try:
        tasks.submit("queue", Work("later"))
        tasks.bind("queue", healthy, concurrency=1)
        tasks.wait_idle(1)
        assert len(failures) == 1
        assert str(failures[0]) == "initial dispatch failed"
        assert stale_calls == ([] if failure_at == 1 else ["first"])
        assert healthy_calls == (
            ["first", "later"] if failure_at == 1 else ["second", "later"]
        )
        assert slots.available == slots.size
        assert slots.subscriptions == 0
        if failure_at == 2:
            assert not returned_before_release
    finally:
        tasks.close()
        slots.close()


def test_idle_waits_for_final_delivery_lease_release() -> None:
    """交付回调已返回但 Slot 引用尚未归还时，后端仍然忙碌。"""

    release_started = ThreadEvent()
    release_allowed = ThreadEvent()
    handler_started = ThreadEvent()
    handler_allowed = ThreadEvent()
    detach_started = ThreadEvent()
    detach_finished = ThreadEvent()

    class DelayedLease:
        """用同步门控制公开 lease 的最终归还。

        Attributes:
            inner: 由默认资源池提供、当前包装器接管的引用。
        """

        def __init__(self, inner: SlotLease) -> None:
            """接管一个引用。

            Args:
                inner: 当前交付持有的引用。
            """

            self.inner = inner

        @property
        def slot(self) -> Slot:
            """返回当前执行槽。

            Returns:
                当前引用保护的 Slot。
            """

            return self.inner.slot

        def retain(self) -> None:
            """为延续工作增加引用。"""

            self.inner.retain()

        def release(self) -> None:
            """等待测试放行后归还当前引用。"""

            release_started.set()
            assert release_allowed.wait(2)
            self.inner.release()

        def execution(self) -> AbstractContextManager[Slot]:
            """委托同槽执行保护。

            Returns:
                串行保护当前 Slot 的上下文管理器。
            """

            return self.inner.execution()

    class DelayedPool(SlotPool):
        """为每次申请提供可控制归还时机的引用。"""

        def try_acquire(self) -> SlotLease | None:
            """立即申请并包装空闲 Slot。

            Returns:
                可控制归还时机的引用，池耗尽时为 None。
            """

            inner = super().try_acquire()
            return None if inner is None else DelayedLease(inner)

    def handle(delivery: Delivery) -> DeliveryResult:
        """等待主线程开始观察清理状态后结束交付。

        Args:
            delivery: 当前投递。

        Returns:
            已完成交付的确认结果。
        """

        del delivery
        handler_started.set()
        assert handler_allowed.wait(2)
        return DeliveryResult.ack()

    slots = DelayedPool(1)
    tasks = memory.TaskBackend()
    detach_thread: Thread | None = None
    try:
        unbind = tasks.bind("queue", handle, concurrency=1, slots=slots)
        tasks.submit("queue", Work("graph"))
        assert handler_started.wait(1)
        handler_allowed.set()
        assert release_started.wait(1)
        assert not tasks.idle
        with pytest.raises(TimeoutError):
            tasks.wait_idle(0)
        assert slots.available == 0

        def detach() -> None:
            """在独立线程注销绑定，验证注销也等待引用清理。"""

            detach_started.set()
            unbind()
            detach_finished.set()

        detach_thread = Thread(target=detach)
        detach_thread.start()
        assert detach_started.wait(1)
        assert not detach_finished.wait(0.03)
    finally:
        handler_allowed.set()
        release_allowed.set()
        if detach_thread is not None:
            detach_thread.join(1)
            assert not detach_thread.is_alive()
        tasks.close()
        slots.close()


def test_consumer_unbind_is_idempotent_and_allows_rebinding() -> None:
    """注销最后消费者后拒绝新投递，重新绑定后恢复接收。"""

    tasks = memory.TaskBackend()
    seen: list[str] = []

    def handler(name: str) -> WorkHandler:
        """为指定消费者创建记录函数。

        Args:
            name: 记录中使用的消费者名称。

        Returns:
            记录并确认交付的处理函数。
        """

        def handle(delivery: Delivery) -> DeliveryResult:
            """记录消费者名称并确认交付。

            Args:
                delivery: 当前交付。

            Returns:
                成功确认结果。
            """

            del delivery
            seen.append(name)
            return DeliveryResult.ack()

        return handle

    try:
        unbind = tasks.bind("queue", handler("old"), concurrency=1)
        tasks.submit("queue", Work("first"))
        tasks.wait_idle(1)
        unbind()
        unbind()
        with pytest.raises(RuntimeError, match="no active consumers"):
            tasks.submit("queue", Work("second"))
        pool = SlotPool(1)
        lease = pool.acquire()
        try:
            with pytest.raises(RuntimeError, match="no active consumers"):
                tasks.submit_local("queue", Work("second"), lease)
            assert lease.slot is not None
        finally:
            lease.release()
            assert pool.available == 1
            pool.close()
        tasks.bind("queue", handler("new"), concurrency=1)
        tasks.submit("queue", Work("second"))
        tasks.wait_idle(1)
        assert seen == ["old", "new"]
    finally:
        if tasks.idle:
            tasks.close()


def test_unbinding_preserves_other_consumer_slot_notifications() -> None:
    """注销共享池的一个消费者后，资源归还仍唤醒其余消费者。"""

    slots = SlotPool(1)
    lease = slots.acquire()
    tasks = memory.TaskBackend()
    seen: list[str] = []

    def handle(delivery: Delivery) -> DeliveryResult:
        """记录并确认分配给剩余消费者的交付。

        Args:
            delivery: 当前投递。

        Returns:
            已完成交付的确认结果。
        """

        seen.append(delivery.work.graph)
        return DeliveryResult.ack()

    try:
        unbind = tasks.bind("queue", handle, concurrency=1, slots=slots)
        tasks.bind("queue", handle, concurrency=1, slots=slots)
        unbind()
        tasks.submit("queue", Work("graph"))
    finally:
        lease.release()
    try:
        tasks.wait_idle(1)
        assert seen == ["graph"]
    finally:
        tasks.close()
        slots.close()


def test_runtime_close_rejects_events_after_last_consumer_unbind() -> None:
    """关闭消费者与路由之间到达的新事件不能留下无人消费的工作。"""

    consumer_unbound = ThreadEvent()
    close_allowed = ThreadEvent()
    close_finished = ThreadEvent()
    failures: list[BaseException] = []

    class PausedTasks(memory.TaskBackend):
        """暂停注销返回，以确定性检查关闭角色之间的投递窗口。"""

        def bind(
            self, queue, handler, *, concurrency, slots=None
        ) -> Callable[[], None]:
            """装饰当前消费者的注销函数。

            Args:
                queue: 当前消费通道。
                handler: 交付处理函数。
                concurrency: 当前本地执行并发上限。
                slots: 申请根执行链资源的资源池。

            Returns:
                注销后等待测试放行的函数。
            """

            detach = super().bind(queue, handler, concurrency=concurrency, slots=slots)

            def unbind() -> None:
                """在消费者已经注销、Router 尚未关闭时暂停。"""

                detach()
                consumer_unbound.set()
                assert close_allowed.wait(2)

            return unbind

    bus = memory.EventBus()
    tasks = PausedTasks()
    runtime = Runtime(
        plugins=(LocalRuntimePlugin(events=bus, tasks=tasks, close_injected=True),)
    )
    runtime.on("value", graph="graph", queue="queue")

    def close() -> None:
        """记录 Runtime 的关闭结果。"""

        try:
            runtime.close()
        except BaseException as exc:
            failures.append(exc)
        finally:
            close_finished.set()

    thread = Thread(target=close)
    thread.start()
    publication_failure: RuntimeError | None = None
    try:
        assert consumer_unbound.wait(1)
        try:
            bus.publish(Event("value"))
        except RuntimeError as exc:
            publication_failure = exc
        if publication_failure is None:
            tasks.bind("queue", lambda delivery: DeliveryResult.ack(), concurrency=1)
    finally:
        close_allowed.set()
        thread.join(1)
    assert close_finished.is_set()
    assert not thread.is_alive()
    assert failures == []
    assert publication_failure is not None
    assert "no active consumers" in str(publication_failure)
    assert tasks.idle


def test_last_consumer_unbind_drains_accepted_continuations() -> None:
    """注销最后消费者时，已运行交付产生的同通道续作仍完成。"""

    root_started = ThreadEvent()
    root_allowed = ThreadEvent()
    detach_started = ThreadEvent()
    child_finished = ThreadEvent()
    failures: list[BaseException] = []
    tasks = memory.TaskBackend()

    def handle(delivery: Delivery) -> DeliveryResult:
        """根交付向同一通道投递一个共享 Slot 的续作。

        Args:
            delivery: 当前根交付或延续交付。

        Returns:
            完成当前交付的确认结果。
        """

        if delivery.work.graph == "root":
            root_started.set()
            assert root_allowed.wait(2)
            lease = delivery.slot_lease
            assert lease is not None
            lease.retain()
            try:
                tasks.submit_local("queue", Work("child"), lease)
            except BaseException:
                lease.release()
                raise
        else:
            child_finished.set()
        return DeliveryResult.ack()

    unbind = tasks.bind("queue", handle, concurrency=1)
    tasks.submit("queue", Work("root"))

    def detach() -> None:
        """等待被注销消费者的根交付和续作全部结束。"""

        detach_started.set()
        try:
            unbind()
        except BaseException as exc:
            failures.append(exc)

    thread = Thread(target=detach)
    try:
        assert root_started.wait(1)
        thread.start()
        assert detach_started.wait(1)
        root_allowed.set()
        thread.join(1)
        completed_before_detach = child_finished.is_set()
        if not tasks.idle:
            tasks.bind("queue", handle, concurrency=1)
        tasks.wait_idle(1)
        assert not thread.is_alive()
        assert failures == []
        assert completed_before_detach
    finally:
        root_allowed.set()
        tasks.close()


def test_runtime_close_drains_inflight_cross_graph_events() -> None:
    """Runtime 关闭开始后，在途 Graph 发出的跨通道事件仍执行完成。"""

    root_started = ThreadEvent()
    root_allowed = ThreadEvent()
    close_started = ThreadEvent()
    close_finished = ThreadEvent()
    seen: list[str] = []
    failures: list[BaseException] = []

    class Root(Node):
        """等待关闭开始后发出跨图事件。"""

        def execute(self, inputs, context) -> None:
            """在关闭期间继续发布已接受执行产生的事件。

            Args:
                inputs: 当前节点输入。
                context: 当前执行上下文。
            """

            del inputs
            root_started.set()
            assert root_allowed.wait(2)
            context.emit("child")

    class Child(Node):
        """记录跨图执行完成。"""

        def execute(self, inputs, context) -> None:
            """记录当前后续执行。

            Args:
                inputs: 当前节点输入。
                context: 当前执行上下文。
            """

            del inputs, context
            seen.append("child")

    runtime = Runtime()
    slots = SlotPool(1)
    runtime.register("root", Graph(entrypoint="node").add("node", Root()))
    runtime.register("child", Graph(entrypoint="node").add("node", Child()))
    runtime.on("root", graph="root", queue="roots", slots=slots)
    runtime.on("child", graph="child", queue="children", slots=slots)
    runtime.emit("root")

    def close() -> None:
        """记录关闭完成及异常。"""

        close_started.set()
        try:
            runtime.close()
        except BaseException as exc:
            failures.append(exc)
        finally:
            close_finished.set()

    thread = Thread(target=close)
    try:
        assert root_started.wait(1)
        thread.start()
        assert close_started.wait(1)
        assert not close_finished.wait(0.03)
    finally:
        root_allowed.set()
        if thread.ident is not None:
            thread.join(1)
        runtime.close()
        slots.close()
    assert not thread.is_alive()
    assert close_finished.is_set()
    assert failures == []
    assert seen == ["child"]
    assert slots.available == 1
