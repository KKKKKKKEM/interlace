"""角色与资源池无需继承默认实现即可装饰或替换的结构化契约测试。"""

import time
from collections import deque
from contextlib import contextmanager
from threading import Condition, RLock

import pytest

from interlace import Graph, Node, Output, Runtime
from interlace.engine import Slot, SlotPool
from interlace.adapters import memory
from interlace.engine.observation import RuntimeEvent
from interlace.engine import InputPolicy
from interlace.plugins import (
    CAP_EVENT_ROUTER,
    CAP_GRAPH_WORKER,
    PluginDescriptor,
    plugin,
)
from interlace.plugins.points import NODE_CALL, RUNTIME_EVENT
from interlace.runtime import EventRouter, GraphWorker
from interlace.spi import RouterRole, SlotProvider, WorkerRole


class RouterWrapper:
    """不继承 EventRouter、仅靠结构化协议满足 RouterRole 的路由包装器。

    Attributes:
        router: 被包装的 EventRouter，所有调用都原样转发给它。
        closed: close() 是否已被调用，供测试断言 Runtime 关闭了包装器。
    """

    def __init__(self, router):
        """包装给定路由角色，并将关闭标记初始化为 False。"""

        self.router = router
        self.closed = False

    @property
    def idle(self):
        """返回被包装路由角色的 idle 状态。"""

        return self.router.idle

    def observe(self, *args, **kwargs):
        """将 observe 调用原样转发给被包装的路由角色并返回其结果。"""

        return self.router.observe(*args, **kwargs)

    def route(self, *args, **kwargs):
        """将 route 调用原样转发给被包装的路由角色并返回其结果。"""

        return self.router.route(*args, **kwargs)

    def emit(self, *args, **kwargs):
        """将 emit 调用原样转发给被包装的路由角色并返回其接受的事件。"""

        return self.router.emit(*args, **kwargs)

    def publish(self, event):
        """将事件原样交给被包装路由角色的 publish。"""

        return self.router.publish(event)

    def publish_local(self, event, lease):
        """将事件与 Slot lease 原样交给被包装路由角色的 publish_local。"""

        return self.router.publish_local(event, lease)

    def wait_idle(self, timeout=None):
        """以相同 timeout 等待被包装路由角色进入空闲。"""

        self.router.wait_idle(timeout)

    def close(self):
        """标记包装器已关闭，再关闭被包装的路由角色。"""

        self.closed = True
        self.router.close()


class WorkerWrapper:
    """不继承 GraphWorker、仅靠结构化协议满足 WorkerRole 的执行器包装器。

    Attributes:
        worker: 被包装的 GraphWorker，所有调用都原样转发给它。
        closed: close() 是否已被调用，供测试断言 Runtime 关闭了包装器。
    """

    def __init__(self, worker):
        """包装给定执行器角色，并将关闭标记初始化为 False。"""

        self.worker = worker
        self.closed = False

    @property
    def idle(self):
        """返回被包装执行器角色的 idle 状态。"""

        return self.worker.idle

    def register(self, *args, **kwargs):
        """将 Graph 注册调用原样转发给被包装的执行器角色。"""

        return self.worker.register(*args, **kwargs)

    def consume(self, *args, **kwargs):
        """将队列消费者注册原样转发给被包装的执行器角色。"""

        return self.worker.consume(*args, **kwargs)

    def start(self, *args, **kwargs):
        """将 start 调用转发给被包装的执行器角色并返回其执行句柄。"""

        return self.worker.start(*args, **kwargs)

    def iter(self, *args, **kwargs):
        """将 iter 调用转发给被包装的执行器角色并返回其同步输出流。"""

        return self.worker.iter(*args, **kwargs)

    def aiter(self, *args, **kwargs):
        """将 aiter 调用转发给被包装的执行器角色并返回其异步输出流。"""

        return self.worker.aiter(*args, **kwargs)

    def get_execution(self, execution_id):
        """从被包装的执行器角色按 ID 查询执行句柄。"""

        return self.worker.get_execution(execution_id)

    def executions(self):
        """返回被包装执行器角色保存的执行记录快照。"""

        return self.worker.executions()

    def wait_idle(self, timeout=None):
        """以相同 timeout 等待被包装执行器角色进入空闲。"""

        self.worker.wait_idle(timeout)

    def begin_close(self):
        """转发关闭前的预备。"""

        self.worker.begin_close()

    def close(self):
        """标记包装器已关闭，再关闭被包装的执行器角色。"""

        self.closed = True
        self.worker.close()


class Echo(Node):
    """将 default 端口输入原样作为输出返回的测试节点。"""

    def execute(self, inputs, context):
        """把 default 端口收到的值包装为 Output 返回。"""

        return Output(inputs["default"])


@pytest.mark.parametrize("wrapped", ["router", "worker", "both"])
def test_roles_can_be_independently_decorated_without_inheritance(wrapped):
    """经 wraps 装饰默认 router、worker 或两者后照常运行，Runtime 关闭装饰后的角色。

    Args:
        wrapped: 被装饰的角色，取值 router、worker 或 both。
    """

    wraps = {
        "router": (CAP_EVENT_ROUTER,),
        "worker": (CAP_GRAPH_WORKER,),
        "both": (CAP_EVENT_ROUTER, CAP_GRAPH_WORKER),
    }[wrapped]
    created: list[object] = []

    def decorate(wrapper):
        """返回记录所创建包装器的装饰函数。"""

        def apply(inner):
            """包装内层角色。"""

            created.append(wrapper(inner))
            return created[-1]

        return apply

    @plugin("example/wrappers", wraps=wraps)
    def wrappers(api):
        """装饰所选角色。

        Args:
            api: 绑定插件身份的扩展接口。
        """

        if CAP_EVENT_ROUTER in wraps:
            api.wrap(CAP_EVENT_ROUTER, decorate(RouterWrapper))
        if CAP_GRAPH_WORKER in wraps:
            api.wrap(CAP_GRAPH_WORKER, decorate(WorkerWrapper))

    runtime = Runtime(plugins=(wrappers,))
    observations: list[RuntimeEvent] = []
    try:
        assert isinstance(runtime.router, RouterRole)
        assert isinstance(runtime.worker, WorkerRole)
        assert (runtime.router in created) == (CAP_EVENT_ROUTER in wraps)
        assert (runtime.worker in created) == (CAP_GRAPH_WORKER in wraps)
        runtime.hooks.on(RUNTIME_EVENT, observations.append)
        runtime.register("echo", Graph(entrypoint="node").add(node=Echo()))
        assert runtime.run("echo", 1) == (Output(1),)
        assert tuple(runtime.iter("echo", 2)) == (Output(2),)
        runtime.on("event", graph="echo", queue="work")
        runtime.emit("event", 3)
        runtime.wait_idle(2)
        assert runtime.executions()[-1].result() == (Output(3),)
        assert observations
    finally:
        runtime.close()
    assert created and all(wrapper.closed for wrapper in created)


def test_replacement_roles_share_runtime_hooks_and_named_values() -> None:
    """在 setup 中以 api.hooks 组装的替代角色，能使用其他插件提供的 selector、中间件与观察者。"""

    tasks = memory.TaskBackend()
    observed: list[RuntimeEvent] = []

    class Selector:
        """仅依赖可用 token 数量的测试选择器。"""

        def select(self, ports, available, config):
            """按声明顺序选择第一个有数据的端口。

            Args:
                ports: 当前节点的输入端口名称。
                available: 各端口可用的 token 数量。
                config: 当前策略的冻结配置。

            Returns:
                待消费的单个端口；无可用端口时返回 None。
            """

            del config
            return next(((port,) for port in ports if available[port]), None)

    class SelectedEcho(Echo):
        """使用插件输入策略的测试节点。

        Attributes:
            input_policy: 由贡献插件提供的具名选择策略。
        """

        input_policy = InputPolicy("example/first")

    class Roles:
        """以宿主钩子总线组装非继承角色，并在 runtime.started 时注册依赖 selector 的 Graph。

        Attributes:
            descriptor: 当前插件提供的角色 capability。
            router: setup 后创建的路由包装器。
            worker: setup 后创建的执行器包装器。
        """

        descriptor = PluginDescriptor(
            "example/roles", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
        )

        def setup(self, api):
            """用宿主钩子总线组装并提供角色，订阅 runtime.started 注册 Graph。

            Args:
                api: 绑定插件身份的扩展接口。
            """

            self.router = RouterWrapper(EventRouter(publisher=tasks, hooks=api.hooks))
            self.worker = WorkerWrapper(
                GraphWorker(consumer=tasks, emit=self.router.publish, hooks=api.hooks)
            )
            api.provide(CAP_EVENT_ROUTER, self.router)
            api.provide(CAP_GRAPH_WORKER, self.worker)
            api.on(RUNTIME_EVENT, self.started, kind="runtime.started")

        def started(self, event):
            """全部插件装配完成后注册使用插件 selector 的 echo Graph，验证 selector 已可用于冻结。"""

            del event
            self.worker.register(
                "echo", Graph(entrypoint="node").add(node=SelectedEcho())
            )

    @plugin("example/features")
    def features(api):
        """提供 selector，登记节点中间件与生命周期观察者。

        Args:
            api: 绑定插件身份的扩展接口。
        """

        api.provide("example/first", Selector())
        api.on(
            NODE_CALL,
            lambda call, call_next: tuple(
                Output(output.value * 2, output.port) for output in call_next(call)
            ),
            graph="echo",
            node="node",
        )
        api.on(RUNTIME_EVENT, observed.append)

    try:
        with Runtime(plugins=(Roles(), features)) as runtime:
            assert runtime.run("echo", 3) == (Output(6),)
            runtime.emit("example/observed", 1)
        assert any(event.event_type == "example/observed" for event in observed)
    finally:
        tasks.close()


class ResourceLease:
    """引用计数的测试 lease，最后一个引用释放时把 Slot 归还给 ResourcePool。

    Attributes:
        provider: 最后一个引用释放时接收归还 Slot 的 ResourcePool。
        slot: 由该 lease 持有、在跨图执行链间共享的 Slot。
        references: 尚未释放的引用数量，初始为 1。
        lock: 保护 references 增减的可重入锁。
        execution_lock: 让 execution() 对同一 Slot 串行使用的可重入锁。
    """

    def __init__(self, provider, slot):
        """以一个初始引用持有来自 provider 的 slot。"""

        self.provider = provider
        self.slot = slot
        self.references = 1
        self.lock = RLock()
        self.execution_lock = RLock()

    def retain(self):
        """为同一逻辑链新增一个必须由接管方释放的引用。

        Raises:
            RuntimeError: 所有引用已经释放后再次调用。
        """

        with self.lock:
            if not self.references:
                raise RuntimeError("released")
            self.references += 1

    def release(self):
        """释放一个引用，最后一个引用释放时把 Slot 放回 provider。

        Raises:
            RuntimeError: 所有引用已经释放后再次调用。
        """

        with self.lock:
            if not self.references:
                raise RuntimeError("released")
            self.references -= 1
            last = not self.references
        if last:
            self.provider.put(self.slot)

    @contextmanager
    def execution(self):
        """在 lease 保护下串行使用同一个 Slot 的执行资源。

        Yields:
            在引用保护与串行锁内使用的测试 Slot。
        """

        self.retain()
        try:
            with self.execution_lock:
                yield self.slot
        finally:
            self.release()


class ResourcePool:
    """不继承 SlotPool、仅靠结构化协议满足 SlotProvider 的固定容量资源池。

    Attributes:
        size: 资源池创建时的 Slot 数量。
        slots: 当前空闲、可被申请的 Slot 队列。
        condition: 保护 slots 与 closed 并在 Slot 归还或关闭时唤醒等待者的条件变量。
        listeners: Slot 归还后依次调用的可用通知回调。
        closed: close() 是否已被调用；关闭后 try_acquire 会抛出异常。
    """

    def __init__(self, size):
        """创建 size 个空闲 Slot 的资源池。"""

        self.size = size
        self.slots = deque(Slot() for _ in range(size))
        self.condition = Condition()
        self.listeners = []
        self.closed = False

    @property
    def available(self):
        """返回当前可申请的执行槽数量。

        Returns:
            当前可供申请的空闲 Slot 数量。
        """

        with self.condition:
            return len(self.slots)

    def try_acquire(self):
        """尝试立即取得 Slot lease，无可用资源时返回 None。

        Returns:
            包装一个空闲 Slot 的 ResourceLease；没有空闲 Slot 时返回 None。

        Raises:
            RuntimeError: 资源池已经关闭。
        """

        with self.condition:
            if self.closed:
                raise RuntimeError("closed")
            return ResourceLease(self, self.slots.popleft()) if self.slots else None

    def acquire(self, timeout=None):
        """等待可用的 Slot，并返回调用方负责释放的 lease。

        Args:
            timeout: 最长等待秒数；None 表示无限等待。

        Returns:
            包装一个空闲 Slot 的 ResourceLease。

        Raises:
            TimeoutError: 在 timeout 内没有 Slot 可用。
            RuntimeError: 资源池已经关闭。
        """

        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            while True:
                lease = self.try_acquire()
                if lease is not None:
                    return lease
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError()
                self.condition.wait(remaining)

    def subscribe_available(self, listener):
        """注册资源可用回调并返回取消订阅函数。

        Args:
            listener: 资源可用时调用的通知函数。

        Returns:
            用于取消当前可用通知订阅的函数。
        """

        self.listeners.append(listener)

        def detach():
            """从 listeners 中移除该回调，重复调用时不做任何事。"""

            if listener in self.listeners:
                self.listeners.remove(listener)

        return detach

    def put(self, slot):
        """把 Slot 放回空闲队列，唤醒等待者并调用所有可用通知回调。"""

        with self.condition:
            self.slots.append(slot)
            self.condition.notify_all()
        for listener in tuple(self.listeners):
            listener()

    def close(self):
        """标记资源池已关闭并唤醒所有等待者。"""

        with self.condition:
            self.closed = True
            self.condition.notify_all()


def test_non_default_slot_provider_and_lease_preserve_cross_graph_resources():
    """验证第三方 SlotProvider 与 lease 保留跨图本地资源。"""

    pool = ResourcePool(1)
    assert isinstance(pool, SlotProvider) and not isinstance(pool, SlotPool)
    seen = []

    class Source(Node):
        """在 Slot 中写入输入值后向 next 事件转发的源节点。"""

        def execute(self, inputs, context):
            """把 default 输入写入 context.slot、记录该 Slot 并发出 next 事件。"""

            assert context.slot is not None
            context.slot["value"] = inputs["default"]
            seen.append(context.slot)
            context.emit("next")

    class Target(Node):
        """读取共享 Slot 中源节点写入值的目标节点。"""

        def execute(self, inputs, context):
            """记录 context.slot 并把源节点写入的 value 作为输出返回。"""

            assert context.slot is not None
            seen.append(context.slot)
            return Output(context.slot["value"])

    with Runtime() as runtime:
        runtime.register("source", Graph(entrypoint="node").add(node=Source()))
        runtime.register("target", Graph(entrypoint="node").add(node=Target()))
        runtime.on("root", graph="source", queue="roots", slots=pool)
        runtime.on("next", graph="target", queue="targets", slots=pool)
        runtime.emit("root", 42)
        runtime.wait_idle(2)
        assert seen[0] is seen[1]
        assert runtime.executions()[-1].result() == (Output(42),)
        assert pool.available == 1
    assert not pool.closed
    pool.close()
