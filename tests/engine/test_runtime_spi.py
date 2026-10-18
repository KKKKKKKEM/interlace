"""Runtime 可替换能力端口的组合契约测试。"""

from __future__ import annotations

import logging
import pickle
from collections import defaultdict
from collections.abc import Callable
from threading import Event as ThreadEvent
from threading import Thread
from typing import Any, cast
from uuid import UUID

import pytest

from interlace import Event, Graph, InputPolicy, Node, Ports, Runtime
from interlace.adapters import memory
from interlace.engine.errors import EventDispatchError
from interlace.engine.hooks import HookRegistry
from interlace.engine.observation import RuntimeEvent, RuntimeEventKind
from interlace.engine.slots import SlotPool
from interlace.runtime import EventRouter, GraphWorker
from interlace.spi import (
    Delivery,
    DeliveryOutcome,
    DeliveryResult,
    EventHandler,
    LocalTaskPublisher,
    SlotLease,
    Work,
    WorkHandler,
    SlotProvider,
)


class EmptyNode(Node):
    """无端口、启动即执行且不做任何事的节点，用于构造单节点测试 Graph。

    Attributes:
        input_ports: 空端口声明，节点不接收输入。
        output_ports: 空端口声明，节点不产生输出。
        input_policy: ON_START，使节点在 Graph 启动时直接执行一次。
    """

    input_ports = Ports()
    output_ports = Ports()
    input_policy = InputPolicy.ON_START

    def execute(self, inputs, context) -> None:
        """丢弃输入与上下文，不产生任何输出。"""

        del inputs, context


class RecordingBus:
    """同步投递并记录事件的 EventBus 替身，证明 Runtime 不依赖默认内存 EventBus。

    Attributes:
        handlers: 按事件类型保存的已订阅回调，"*" 键保存通配订阅。
        events: 每次 publish 经 pickle 往返后的事件副本，按发布顺序记录。
        closed: close() 是否被调用，用于断言角色是否关闭了注入的总线。
    """

    def __init__(self) -> None:
        """创建空的订阅表与事件记录，并标记为未关闭。"""

        self.handlers: dict[str, list[EventHandler]] = defaultdict(list)
        self.events: list[Event] = []
        self.closed = False

    @property
    def idle(self) -> bool:
        """始终返回 True，因为 publish 会同步完成全部投递。"""

        return True

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        *,
        subscription: str | None = None,
    ) -> Callable[[], None]:
        """按事件类型登记回调；不模拟订阅组，同名订阅也各自收到事件。

        Args:
            event_type: 回调监听的事件类型，"*" 表示通配。
            handler: 收到事件后调用的回调。
            subscription: 订阅组名称，本替身直接丢弃。

        Returns:
            仅移除本次登记回调的注销函数。
        """

        del subscription

        def registered(event: Event) -> None:
            """转调 handler，使每次 subscribe 都产生可单独移除的回调对象。"""

            handler(event)

        self.handlers[event_type].append(registered)

        def unsubscribe() -> None:
            """从该事件类型的回调列表移除本次登记；已移除时不做任何事。"""

            if registered in self.handlers[event_type]:
                self.handlers[event_type].remove(registered)

        return unsubscribe

    def publish(self, event: Event) -> None:
        """经 pickle 往返模拟跨进程传输，记录副本后同步调用该类型及通配回调。"""

        transported = pickle.loads(pickle.dumps(event))
        self.events.append(transported)
        handlers = tuple(self.handlers[transported.type]) + tuple(self.handlers["*"])
        for handler in handlers:
            handler(transported)

    def wait_idle(self, timeout: float | None = None) -> None:
        """所有投递已同步完成，直接忽略 timeout 返回。"""

        del timeout

    def close(self) -> None:
        """仅把 closed 标记为 True。"""

        self.closed = True


class RecordingTasks:
    """在 submit 中同步消费 Work 的任务后端替身，模拟可由 Redis/MQ 替换的实现。

    Attributes:
        handlers: 队列名到当前绑定消费函数的映射，后绑定者覆盖先绑定者。
        submitted: 按提交顺序记录的 (队列名, Work) 对。
        closed: close() 是否被调用，用于断言角色是否关闭了注入的后端。
    """

    def __init__(self) -> None:
        """创建空的绑定表与提交记录，并标记为未关闭。"""

        self.handlers: dict[str, Callable[[Work], None]] = {}
        self.submitted: list[tuple[str, Work]] = []
        self.closed = False

    @property
    def idle(self) -> bool:
        """始终返回 True，因为 submit 会同步执行完 Work。"""

        return True

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
        slots: SlotProvider | None = None,
    ) -> Callable[[], None]:
        """为队列登记同步消费函数，每个 Work 持有一个 Slot 租约交给 handler 处理。

        Args:
            queue: 被绑定的队列名。
            handler: 接收 Delivery 并返回 DeliveryResult 的消费函数。
            concurrency: 未提供 slots 时用于新建 SlotPool 的容量。
            slots: 自定义 Slot 来源，None 时按 concurrency 新建 SlotPool。

        Returns:
            仅当队列仍绑定本次消费函数时才解除绑定的函数。
        """

        if slots is None:
            slots = SlotPool(concurrency)

        def handle(work: Work) -> None:
            """pickle 往返复制 Work，持有 lease 调用 handler，结束后释放 lease。

            Raises:
                RuntimeError: handler 返回非 ACK 且未携带 error。
                Exception: handler 返回非 ACK 时携带的 error 原样抛出。
            """

            transported = pickle.loads(pickle.dumps(work))
            lease = slots.acquire()
            try:
                result = handler(Delivery(transported, slot_lease=lease))
                if result.outcome is not DeliveryOutcome.ACK:
                    raise result.error or RuntimeError(
                        f"work handler returned {result.outcome.value}"
                    )
            finally:
                lease.release()

        self.handlers[queue] = handle

        def unbind() -> None:
            """只移除当前绑定，不影响后来替换的消费者。"""

            if self.handlers.get(queue) is handle:
                del self.handlers[queue]

        return unbind

    def submit(self, queue: str, work: Work) -> None:
        """记录提交后立即同步调用该队列当前绑定的消费函数。

        Raises:
            KeyError: 队列尚未绑定消费函数。
        """

        self.submitted.append((queue, work))
        self.handlers[queue](work)

    def wait_idle(self, timeout: float | None = None) -> None:
        """所有 Work 已同步执行完，直接忽略 timeout 返回。"""

        del timeout

    def close(self) -> None:
        """仅把 closed 标记为 True。"""

        self.closed = True


class RecordingExecutor:
    """记录每次执行的 Graph 名称与输入、不继承 Engine 的 GraphExecutor 替身。

    Attributes:
        calls: 按执行顺序记录的 (Graph 注册名, 输入) 对。
        closed: close() 是否被调用，用于断言角色是否关闭了注入的执行器。
    """

    def __init__(self) -> None:
        """创建空的调用记录，并标记为未关闭。"""

        self.calls: list[tuple[str, object]] = []
        self.closed = False

    def execute(
        self,
        name: str,
        graph: Graph,
        inputs: object,
        emit: Callable[[Event], None],
        plan=None,
        *,
        slot=None,
        options=None,
        execution,
    ) -> None:
        """为入口节点执行一个空的 execution 步骤并记录名称与输入；不发布事件。"""

        del emit, plan, slot
        with execution.step(graph.entrypoint):
            pass
        self.calls.append((name, inputs))

    def close(self) -> None:
        """仅把 closed 标记为 True。"""

        self.closed = True


class HookableRecordingExecutor(RecordingExecutor):
    """在 RecordingExecutor 上组合 HookRegistry，不继承 Engine 也能提供 attach。

    Attributes:
        hooks: 保存 attach 登记的 Hook，测试通过其 snapshot 检查登记与卸载。
    """

    def __init__(self) -> None:
        """初始化调用记录并创建独立的 HookRegistry。"""

        super().__init__()
        self.hooks = HookRegistry()

    def attach(self, hook, *, phase=None, graph=None, node=None):
        """把 Hook 及其 phase、graph、node 作用域原样转交 HookRegistry 登记。

        Returns:
            HookRegistry.attach 返回的句柄，可 detach 卸载本次登记。
        """

        return self.hooks.attach(
            hook,
            phase=phase,
            graph=graph,
            node=node,
        )

    def close(self) -> None:
        """先关闭 HookRegistry，再标记执行器已关闭。"""

        self.hooks.close()
        super().close()


def test_runtime_composes_replaceable_capabilities() -> None:
    """替换传输、任务和执行器不改变 Runtime/Graph 使用 API。"""

    bus = RecordingBus()
    tasks = RecordingTasks()
    executor = RecordingExecutor()
    graph = Graph(entrypoint="empty").add("empty", EmptyNode())

    router = EventRouter(events=bus, publisher=tasks)
    worker = GraphWorker(consumer=tasks, executor=executor, emit=router.publish)
    with Runtime(router=router, worker=worker) as runtime:
        runtime.register("work.graph", graph)
        runtime.route(
            "work.created",
            graph="work.graph",
            queue="work-tasks",
        )
        runtime.consume("work-tasks", concurrency=8)
        runtime.emit("work.created", {"id": 1})
        runtime.wait_idle()

    assert bus.events == [Event("work.created", {"id": 1})]
    assert len(tasks.submitted) == 1
    queue, work = tasks.submitted[0]
    assert queue == "work-tasks"
    assert work.graph == "work.graph"
    assert work.inputs == {"id": 1}
    assert work.id
    assert tasks.submitted[0][1].trigger == Event("work.created", {"id": 1})
    assert executor.calls == [("work.graph", {"id": 1})]
    assert not bus.closed and not tasks.closed and not executor.closed


def test_custom_executor_can_explicitly_support_runtime_hooks() -> None:
    """Hook 是结构化可选能力，不要求自定义执行器继承 Engine。"""

    tasks = RecordingTasks()
    executor = HookableRecordingExecutor()
    worker = GraphWorker(consumer=tasks, executor=executor)
    worker.register("work.graph", Graph(entrypoint="empty").add(empty=EmptyNode()))

    handle = worker.attach(lambda call: call, graph="work.graph", node="empty")

    assert executor.hooks.snapshot("work.graph")
    handle.detach()
    assert executor.hooks.snapshot("work.graph") == ()
    worker.close()


def test_custom_executor_without_hook_capability_is_rejected() -> None:
    """普通 GraphExecutor 仍可执行，只在请求可选 Hook 能力时失败。"""

    tasks = RecordingTasks()
    worker = GraphWorker(consumer=tasks, executor=RecordingExecutor())
    worker.register("work.graph", Graph(entrypoint="empty").add(empty=EmptyNode()))

    with pytest.raises(TypeError, match="does not support hooks"):
        worker.attach(lambda call: call, graph="work.graph")

    worker.close()


def test_runtime_explicitly_exposes_composed_roles() -> None:
    """Runtime 保存并代理调用方显式传入的 Router 与 Worker。"""

    bus = RecordingBus()
    tasks = RecordingTasks()
    executor = RecordingExecutor()
    router = EventRouter(events=bus, publisher=tasks)
    worker = GraphWorker(consumer=tasks, executor=executor, emit=router.publish)

    runtime = Runtime(router=router, worker=worker)

    assert runtime.router is router
    assert runtime.worker is worker
    runtime.close()


def test_runtime_rejects_partial_role_composition() -> None:
    """组合根不接受缺少 Router 或 Worker 的残缺配置。"""

    tasks = memory.TaskBackend()
    router = EventRouter(publisher=tasks)
    with pytest.raises(TypeError, match="both router and worker"):
        Runtime(router=router)
    router.close()
    tasks.close()


def test_roles_can_close_injected_components() -> None:
    """角色可以显式接管注入组件的生命周期。"""

    bus = RecordingBus()
    publisher = RecordingTasks()
    consumer = RecordingTasks()
    executor = RecordingExecutor()
    router = EventRouter(
        events=bus,
        publisher=publisher,
        close_injected=True,
    )
    worker = GraphWorker(
        consumer=consumer,
        executor=executor,
        emit=router.publish,
        close_injected=True,
    )
    runtime = Runtime(router=router, worker=worker)

    runtime.close()

    assert bus.closed and publisher.closed and consumer.closed and executor.closed


def test_work_generates_id_and_accepts_explicit_id() -> None:
    """Work 默认生成 ID，也允许传输适配器恢复已有 ID。"""

    generated = Work("graph")
    restored = Work("graph", id="work-42")

    assert str(UUID(generated.id)) == generated.id
    assert restored.id == "work-42"


def test_named_event_subscriptions_compete_and_distinct_ones_broadcast() -> None:
    """同名订阅轮流消费，不同订阅各自收到一份 Event。"""

    bus = memory.EventBus()
    first: list[int] = []
    second: list[int] = []
    audit: list[int] = []
    bus.subscribe(
        "value", lambda event: first.append(event.payload), subscription="work"
    )
    bus.subscribe(
        "value", lambda event: second.append(event.payload), subscription="work"
    )
    bus.subscribe(
        "value", lambda event: audit.append(event.payload), subscription="audit"
    )

    for value in range(4):
        bus.publish(Event("value", value))

    assert first == [0, 2]
    assert second == [1, 3]
    assert audit == [0, 1, 2, 3]


def test_router_and_worker_can_use_separate_runtime_roles() -> None:
    """Router 无需注册 Graph，Worker 无需订阅 Event；关闭角色不关闭注入的传输。"""

    bus = memory.EventBus()
    tasks = memory.TaskBackend()
    executor = RecordingExecutor()
    router = EventRouter(events=bus, publisher=tasks)
    worker = GraphWorker(consumer=tasks, executor=executor, emit=router.publish)
    worker.register("work.graph", Graph(entrypoint="empty").add("empty", EmptyNode()))
    worker.consume("work", concurrency=2)
    router.route("work.created", graph="work.graph", queue="work")
    router.emit("work.created", {"id": 1})

    tasks.wait_idle()
    router.close()
    worker.close()

    assert executor.calls == [("work.graph", {"id": 1})]
    received: list[Event] = []
    bus.subscribe("still.open", received.append)
    bus.publish(Event("still.open"))
    assert received == [Event("still.open")]
    unbind = tasks.bind(
        "still.open", lambda delivery: DeliveryResult.ack(), concurrency=1
    )
    tasks.submit("still.open", Work("graph"))
    tasks.wait_idle(1)
    unbind()
    bus.close()
    tasks.close()


def test_memory_event_bus_tracks_active_dispatches() -> None:
    """同步 handler 尚未返回时，EventBus 不能报告空闲。"""

    bus = memory.EventBus()
    started = ThreadEvent()
    release = ThreadEvent()

    def slow_handler(event: Event) -> None:
        """通知测试已进入回调，然后阻塞到 release 被设置，使分发保持进行中。"""

        del event
        started.set()
        release.wait()

    bus.subscribe("slow", slow_handler)
    publisher = Thread(target=bus.publish, args=(Event("slow"),))
    publisher.start()
    assert started.wait(1)
    try:
        assert not bus.idle
        with pytest.raises(TimeoutError):
            bus.wait_idle(0)
    finally:
        release.set()
        publisher.join(1)

    assert not publisher.is_alive()
    bus.wait_idle(0)
    assert bus.idle


def test_wildcard_event_reaches_wildcard_subscriber_once() -> None:
    """事件类型本身为通配符时不重复拼接同一订阅列表。"""

    bus = memory.EventBus()
    received: list[Event] = []
    bus.subscribe("*", received.append)

    event = Event("*", "payload")
    bus.publish(event)

    assert received == [event]


def test_event_bus_continues_after_a_handler_failure() -> None:
    """EventBus 层的单个订阅者失败不阻断其他订阅者，全部调用后抛出首个失败。"""

    bus = memory.EventBus()
    received: list[Event] = []

    def fail(event: Event) -> None:
        """模拟订阅者失败。

        Raises:
            ValueError: 每次调用都抛出 "observer failed"。
        """

        del event
        raise ValueError("observer failed")

    bus.subscribe("value", fail)
    bus.subscribe("value", received.append)

    with pytest.raises(ValueError, match="observer failed"):
        bus.publish(Event("value", "payload"))
    assert received == [Event("value", "payload")]


def test_task_backend_raises_first_failure_and_logs_the_rest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """wait_idle 只抛出首个失败并清空，同期其他失败写入日志而不是静默丢弃。"""

    backend = memory.TaskBackend()

    def fail(delivery: Delivery) -> DeliveryResult:
        """以 Work 输入作为消息抛出异常。

        Raises:
            ValueError: 总是抛出。
        """

        raise ValueError(delivery.work.inputs)

    backend.bind("failures", fail, concurrency=1)
    with caplog.at_level(logging.ERROR, logger="interlace.adapters.memory"):
        backend.submit("failures", Work("graph", "first"))
        backend.submit("failures", Work("graph", "second"))
        with pytest.raises(ValueError, match="first"):
            backend.wait_idle(1)
    backend.wait_idle(0)
    logged = [record.exc_info[1] for record in caplog.records if record.exc_info]
    assert [str(error) for error in logged] == ["second"]
    backend.close()


def test_task_backend_requires_explicit_delivery_result() -> None:
    """handler 返回 None 而非 DeliveryResult 时，wait_idle 抛出 TypeError。"""

    backend = memory.TaskBackend()
    backend.bind("invalid", cast(Any, lambda delivery: None), concurrency=1)
    backend.submit("invalid", Work("graph"))

    with pytest.raises(TypeError, match="must return DeliveryResult"):
        backend.wait_idle()
    backend.close()


def test_ack_delivery_result_rejects_an_error() -> None:
    """ACK 不得携带会被后端静默忽略的异常。"""

    with pytest.raises(ValueError, match="must not contain an error"):
        DeliveryResult(DeliveryOutcome.ACK, ValueError("lost"))


class CountingLease:
    """记录 retain/release 次数的 SlotLease 替身。

    Attributes:
        retained: retain 调用次数。
        released: release 调用次数。
        fail_release: release 计数后是否抛出 RuntimeError。
    """

    def __init__(self, *, fail_release: bool = False) -> None:
        """创建计数为零的替身。

        Args:
            fail_release: release 计数后是否抛出 RuntimeError。
        """

        self.retained = 0
        self.released = 0
        self.fail_release = fail_release

    @property
    def slot(self) -> object:
        """返回占位 Slot；本文件的用例不读取其内容。"""

        return object()

    def retain(self) -> None:
        """记录一次 retain。"""

        self.retained += 1

    def release(self) -> None:
        """记录一次 release，并按配置抛出异常。

        Raises:
            RuntimeError: fail_release 为 True。
        """

        self.released += 1
        if self.fail_release:
            raise RuntimeError("release failed")

    def execution(self) -> Any:
        """本文件的用例不进入 execution。

        Raises:
            NotImplementedError: 总是抛出。
        """

        raise NotImplementedError


class RecordingLocalPublisher:
    """区分普通提交与本地 Slot 链提交的 LocalTaskPublisher 替身。

    Attributes:
        remote: submit 收到的 (队列, Graph 名称)。
        local: submit_local 收到的 (队列, Graph 名称)。
        fail_local: submit_local 是否抛出 RuntimeError。
    """

    def __init__(self, *, fail_local: bool = False) -> None:
        """创建空记录。

        Args:
            fail_local: submit_local 是否抛出 RuntimeError。
        """

        self.remote: list[tuple[str, str]] = []
        self.local: list[tuple[str, str]] = []
        self.fail_local = fail_local

    def submit(self, queue: str, work: Work) -> None:
        """记录普通提交。"""

        self.remote.append((queue, work.graph))

    def submit_local(self, queue: str, work: Work, lease: SlotLease) -> None:
        """记录本地提交并立即归还接管的引用，或按配置拒绝。

        Raises:
            RuntimeError: fail_local 为 True。
        """

        if self.fail_local:
            raise RuntimeError("local submission failed")
        self.local.append((queue, work.graph))
        lease.release()

    def close(self) -> None:
        """替身没有需要释放的资源。"""


def test_memory_backend_rejects_queues_without_consumers() -> None:
    """从未绑定或已注销全部消费者的通道立即拒绝 Work，不留下无人处理的排队。"""

    tasks = memory.TaskBackend()
    with pytest.raises(RuntimeError, match="'never' has no active consumers"):
        tasks.submit("never", Work("graph"))
    unbind = tasks.bind("queue", lambda delivery: DeliveryResult.ack(), concurrency=1)
    unbind()
    with pytest.raises(RuntimeError, match="'queue' has no active consumers"):
        tasks.submit("queue", Work("graph"))
    with pytest.raises(RuntimeError, match="'queue' has no active consumers"):
        tasks.submit_local("queue", Work("graph"), CountingLease())
    assert tasks.idle
    tasks.close()


def test_route_without_consumer_fails_fast_and_runtime_closes_promptly() -> None:
    """只建立 route 而没有消费者时，emit 立即失败，wait_idle/close 不会挂起。"""

    runtime = Runtime()
    runtime.register("target", Graph(entrypoint="empty").add("empty", EmptyNode()))
    runtime.route("evt", graph="target", queue="unconsumed")
    with pytest.raises(EventDispatchError, match="no active consumers"):
        runtime.emit("evt")
    closer = Thread(target=runtime.close)
    closer.start()
    closer.join(2)
    assert not closer.is_alive()


def test_observer_failure_is_logged_and_isolated_from_emit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """observe 观察者失败只写日志；其他观察者与 route 照常收到 Event，emit 正常返回。"""

    publisher = RecordingLocalPublisher()
    router = EventRouter(publisher=publisher)
    received: list[Event] = []

    def fail(event: Event) -> None:
        """模拟失败的观察者。

        Raises:
            ValueError: 总是抛出。
        """

        del event
        raise ValueError("observer failed")

    router.observe("value", fail)
    router.observe("value", received.append)
    router.route("value", graph="graph", queue="queue")
    with caplog.at_level(logging.ERROR, logger="interlace.runtime.router"):
        event = router.emit("value", 1)

    assert received == [event]
    assert publisher.remote == [("queue", "graph")]
    assert [
        str(record.exc_info[1]) for record in caplog.records if record.exc_info
    ] == ["observer failed"]
    router.close()


def test_route_submission_failure_propagates_as_dispatch_error() -> None:
    """route handler 的 Work 提交失败仍以 EventDispatchError 传回 emit 调用方。"""

    tasks = memory.TaskBackend()
    router = EventRouter(publisher=tasks)
    router.route("value", graph="graph", queue="unconsumed")
    with pytest.raises(EventDispatchError) as raised:
        router.emit("value")
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "no active consumers" in str(raised.value.__cause__)
    router.close()
    tasks.close()


def test_observer_emit_does_not_inherit_source_slot_lease() -> None:
    """只有 route handler 的提交延续源 lease；观察者内部发布的 Event 开始新的 Slot 链。"""

    publisher = RecordingLocalPublisher()
    assert isinstance(publisher, LocalTaskPublisher)
    router = EventRouter(publisher=publisher)
    router.route("parent", graph="parent.graph", queue="queue")
    router.route("child", graph="child.graph", queue="queue")
    router.observe("parent", lambda event: router.emit("child"))
    lease = CountingLease()

    router.publish_local(Event("parent"), lease)

    assert publisher.local == [("queue", "parent.graph")]
    assert publisher.remote == [("queue", "child.graph")]
    assert (lease.retained, lease.released) == (1, 1)
    router.close()


def test_rejected_local_submission_releases_its_reference_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """submit_local 失败时 Router 只释放一次自己 retain 的引用，释放失败不覆盖原始失败。"""

    router = EventRouter(publisher=RecordingLocalPublisher(fail_local=True))
    router.route("value", graph="graph", queue="queue")
    lease = CountingLease(fail_release=True)

    with (
        caplog.at_level(logging.ERROR, logger="interlace.runtime.router"),
        pytest.raises(EventDispatchError, match="local submission failed"),
    ):
        router.publish_local(Event("value"), lease)

    assert (lease.retained, lease.released) == (1, 1)
    assert any(
        record.exc_info and str(record.exc_info[1]) == "release failed"
        for record in caplog.records
    )
    router.close()


def test_plain_publisher_never_touches_the_source_lease() -> None:
    """发布端不实现 LocalTaskPublisher 时提交根 Work，不 retain 或 release 源 lease。"""

    tasks = RecordingTasks()
    tasks.bind("queue", lambda delivery: DeliveryResult.ack(), concurrency=1)
    assert not isinstance(tasks, LocalTaskPublisher)
    router = EventRouter(publisher=tasks)
    router.route("value", graph="graph", queue="queue")
    lease = CountingLease()

    router.publish_local(Event("value"), lease)

    assert [queue for queue, _ in tasks.submitted] == ["queue"]
    assert (lease.retained, lease.released) == (0, 0)
    router.close()


def test_event_published_precedes_the_work_it_causes() -> None:
    """EVENT_PUBLISHED 先于同一 Event 引起的 WORK_SUBMITTED 被观察到。"""

    router = EventRouter(publisher=RecordingLocalPublisher())
    observed: list[RuntimeEvent] = []
    router.observe_runtime(observed.append)
    router.route("value", graph="graph", queue="queue")

    router.emit("value")

    assert [event.kind for event in observed] == [
        RuntimeEventKind.EVENT_PUBLISHED,
        RuntimeEventKind.WORK_SUBMITTED,
    ]
    router.close()


def test_task_backend_records_drain_failures_and_keeps_waking_waiters(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Slot 申请在完成回调中失败时记录失败并唤醒等待方，后续提交仍能推进排队 Work。"""

    second_failure = ThreadEvent()

    class FlakyPool(SlotPool):
        """第 3、4 次 try_acquire 抛错的 SlotPool。

        Attributes:
            calls: try_acquire 调用次数。
        """

        def __init__(self) -> None:
            """创建容量为 1 的池。"""

            super().__init__(1)
            self.calls = 0

        def try_acquire(self) -> SlotLease | None:
            """按调用序号抛错，其余调用委托默认实现。

            Raises:
                RuntimeError: 第 3、4 次调用。
            """

            self.calls += 1
            if self.calls in (3, 4):
                if self.calls == 4:
                    second_failure.set()
                raise RuntimeError(f"acquire failed #{self.calls}")
            return super().try_acquire()

    handled: list[object] = []
    queued = ThreadEvent()
    pool = FlakyPool()
    tasks = memory.TaskBackend()

    def handle(delivery: Delivery) -> DeliveryResult:
        """记录处理顺序；a 等到 b 排队后再完成。"""

        handled.append(delivery.work.inputs)
        if delivery.work.inputs == "a":
            assert queued.wait(2)
        return DeliveryResult.ack()

    tasks.bind("queue", handle, concurrency=2, slots=pool)
    with caplog.at_level(logging.ERROR, logger="interlace.adapters.memory"):
        # 第 1 次申请成功执行 a，第 2 次池耗尽使 b 排队；a 归还后第 3、4 次申请失败。
        tasks.submit("queue", Work("graph", "a"))
        tasks.submit("queue", Work("graph", "b"))
        queued.set()
        assert second_failure.wait(2)
        tasks.submit("queue", Work("graph", "c"))
        with pytest.raises(RuntimeError, match="acquire failed #3"):
            tasks.wait_idle(2)

    assert handled == ["a", "b", "c"]
    assert [
        str(record.exc_info[1]) for record in caplog.records if record.exc_info
    ] == ["acquire failed #4"]
    tasks.close()
    pool.close()


def test_concurrent_close_waits_for_the_same_drain() -> None:
    """并发 close 都在同一次排空完成后返回，关闭后拒绝新提交。"""

    started = ThreadEvent()
    allowed = ThreadEvent()
    tasks = memory.TaskBackend()

    def block(delivery: Delivery) -> DeliveryResult:
        """阻塞到测试放行。"""

        del delivery
        started.set()
        assert allowed.wait(2)
        return DeliveryResult.ack()

    tasks.bind("queue", block, concurrency=1)
    tasks.submit("queue", Work("graph"))
    assert started.wait(1)
    closers = [Thread(target=tasks.close) for _ in range(2)]
    for closer in closers:
        closer.start()
    for closer in closers:
        closer.join(0.05)
    assert all(closer.is_alive() for closer in closers)
    allowed.set()
    for closer in closers:
        closer.join(2)
    assert not any(closer.is_alive() for closer in closers)
    with pytest.raises(RuntimeError, match="closed"):
        tasks.submit("queue", Work("graph"))
    tasks.close()
