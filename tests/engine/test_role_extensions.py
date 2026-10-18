"""角色与资源池无需继承默认实现即可替换的结构化契约测试。"""

from collections import deque
from contextlib import contextmanager
from threading import Condition, RLock
import time

import pytest

from interlace import Graph, Node, Output, Runtime, Slot, SlotPool
from interlace.adapters import memory
from interlace.engine.observation import RuntimeEvent
from interlace.engine.policies import PolicyRef
from interlace.plugins import (
    CAP_EVENT_ROUTER,
    CAP_GRAPH_WORKER,
    ContributionPlugin,
    NodeHookContribution,
    PluginDescriptor,
)
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

        self.router.publish(event)

    def observe_runtime(self, observer):
        """将运行时观察者转交给被包装的路由角色注册并返回其结果。"""

        return self.router.observe_runtime(observer)

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

    def attach(self, *args, **kwargs):
        """将扩展回调注册转发给被包装的执行器角色并返回其卸载句柄。"""

        return self.worker.attach(*args, **kwargs)

    def contribute_hook(self, *args, **kwargs):
        """将节点 Hook 贡献转发给被包装的执行器角色并返回其卸载函数。"""

        return self.worker.contribute_hook(*args, **kwargs)

    def register_policy(self, *args, **kwargs):
        """将输入选择策略注册转发给被包装的执行器角色。"""

        return self.worker.register_policy(*args, **kwargs)

    def observe_runtime(self, observer):
        """将运行时观察者转交给被包装的执行器角色注册并返回其结果。"""

        return self.worker.observe_runtime(observer)

    def wait_idle(self, timeout=None):
        """以相同 timeout 等待被包装执行器角色进入空闲。"""

        self.worker.wait_idle(timeout)

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
@pytest.mark.parametrize("plugin", [False, True])
def test_roles_can_be_independently_decorated_and_composed_without_inheritance(
    wrapped, plugin
):
    """验证包装 router、worker 或两者后，经直接注入或插件装配均可运行并被 Runtime 关闭。

    Args:
        wrapped: 替换为包装器的角色，取值 router、worker 或 both。
        plugin: 为 True 时通过插件提供角色，否则直接传给 Runtime 构造函数。
    """

    tasks = memory.TaskBackend()
    router = EventRouter(publisher=tasks)
    worker = GraphWorker(
        consumer=tasks, emit=router.publish, emit_local=router.publish_local
    )
    selected_router = RouterWrapper(router) if wrapped in ("router", "both") else router
    selected_worker = WorkerWrapper(worker) if wrapped in ("worker", "both") else worker
    assert isinstance(selected_router, RouterRole)
    assert isinstance(selected_worker, WorkerRole)

    class Roles:
        """通过插件 capability 提供本用例所选路由与执行器角色的插件。

        Attributes:
            descriptor: 声明提供 CAP_EVENT_ROUTER 与 CAP_GRAPH_WORKER 的插件描述。
        """

        descriptor = PluginDescriptor(
            "example/roles", "1", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
        )

        def setup(self, context):
            """将所选的路由与执行器角色登记为对应 capability。"""

            context.provide(CAP_EVENT_ROUTER, selected_router)
            context.provide(CAP_GRAPH_WORKER, selected_worker)

        def start(self, context):
            """启动阶段无需额外操作。"""

            pass

        def stop(self, context):
            """先关闭所选执行器角色，再关闭所选路由角色。"""

            selected_worker.close()
            selected_router.close()

    runtime = (
        Runtime(plugins=(Roles(),))
        if plugin
        else Runtime(router=selected_router, worker=selected_worker)
    )
    observations: list[RuntimeEvent] = []
    try:
        runtime.observe_runtime(lambda event: observations.append(event))
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
        tasks.close()
    if wrapped in ("router", "both"):
        assert isinstance(selected_router, RouterWrapper)
        assert selected_router.closed
    if wrapped in ("worker", "both"):
        assert isinstance(selected_worker, WorkerWrapper)
        assert selected_worker.closed


def test_role_plugins_install_contributions_before_start() -> None:
    """验证替代角色在插件启动前取得 selector、延迟 Hook 和观察者贡献。"""

    tasks = memory.TaskBackend()
    router = RouterWrapper(EventRouter(publisher=tasks))
    worker = WorkerWrapper(GraphWorker(consumer=tasks, emit=router.publish))
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

        input_policy = PolicyRef("example/first")

    class Roles:
        """提供非继承角色，并在启动时注册依赖贡献的 Graph。

        Attributes:
            descriptor: 当前插件提供的角色 capability。
        """

        descriptor = PluginDescriptor(
            "example/roles", "1", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
        )

        def setup(self, context):
            """提供本次测试使用的公开角色。

            Args:
                context: 宿主提供的能力注册上下文。
            """

            context.provide(CAP_EVENT_ROUTER, router)
            context.provide(CAP_GRAPH_WORKER, worker)

        def start(self, context):
            """在启动阶段注册使用插件 selector 的 echo Graph，验证 selector 已可用于冻结。"""

            del context
            worker.register("echo", Graph(entrypoint="node").add(node=SelectedEcho()))

        def stop(self, context):
            """关闭本插件提供的执行器与路由包装器。"""

            del context
            worker.close()
            router.close()

    contributions = ContributionPlugin(
        "example/features",
        selectors={"example/first": Selector()},
        hooks={
            "example/double": NodeHookContribution(
                lambda call, output: Output(output.value * 2, output.port),
                phase="exit",
                graph="echo",
                node="node",
            )
        },
        observers={"example/trace": observed.append},
    )
    try:
        with Runtime(plugins=(Roles(), contributions)) as runtime:
            assert runtime.run("echo", 3) == (Output(6),)
            runtime.emit("example/observed", 1)
        assert observed
        assert any(event.event_type == "example/observed" for event in observed)
    finally:
        tasks.close()
        worker.close()
        router.close()


def test_deferred_hook_can_be_detached_before_graph_registration() -> None:
    """验证提前注销延迟 Hook 后注册同名 Graph 不会重新激活该 Hook。"""

    tasks = memory.TaskBackend()
    worker = GraphWorker(consumer=tasks)
    try:
        detach = worker.contribute_hook(
            lambda call, output: Output(output.value * 2),
            phase="exit",
            graph="echo",
            node="node",
        )
        detach()
        detach()
        worker.register("echo", Graph(entrypoint="node").add(node=Echo()))
        assert worker.start("echo", 3).result() == (Output(3),)
    finally:
        worker.close()
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
