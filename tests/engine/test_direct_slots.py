"""直接执行在调用方 SlotProvider 租用的 Slot 内运行的契约测试。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from threading import Event as ThreadEvent, Lock, Thread
from typing import Any

import pytest

from interlace import Graph, Node, Output, Runtime
from interlace.engine import Execution, Slot, SlotPool
from interlace.engine.errors import SlotProviderClosedError
from interlace.engine.observation import RuntimeEvent, RuntimeEventKind
from interlace.plugins.points import RUNTIME_EVENT
from interlace.runtime import LocalRuntimePlugin
from interlace.spi import SlotLease


class ReadSlot(Node):
    """返回当前 Slot 身份的节点。"""

    outputs = {"default": str}

    def execute(self, inputs: Any, context: Any) -> Output:
        """输出 context.slot 的 id。"""

        del inputs
        assert context.slot is not None
        return Output(context.slot.id)


class Hold(Node):
    """登记进入并阻塞到测试放行的节点，用于观察执行重叠。

    Attributes:
        entered: 每次进入 execute 时置位的事件。
        release: 测试放行所有阻塞调用的事件。
        active: 当前正在 execute 内的调用数。
        overlap: 观察到的最大并发 execute 数。
        _lock: 保护 active 与 overlap。
    """

    outputs = {"default": str}

    def __init__(self) -> None:
        """创建未放行的同步原语。"""

        self.entered = ThreadEvent()
        self.release = ThreadEvent()
        self.active = 0
        self.overlap = 0
        self._lock = Lock()

    def execute(self, inputs: Any, context: Any) -> Output:
        """记录并发度后等待放行，输出 Slot id。"""

        del inputs
        assert context.slot is not None
        with self._lock:
            self.active += 1
            self.overlap = max(self.overlap, self.active)
        self.entered.set()
        try:
            assert self.release.wait(5)
        finally:
            with self._lock:
                self.active -= 1
        return Output(context.slot.id)


class CountingProvider:
    """委托 SlotPool 并统计活跃订阅的 SlotProvider 测试替身，可注入申请行为。

    Attributes:
        pool: 被委托的默认 SlotPool。
        subscriptions: 尚未注销的可用通知数量。
        acquire_hook: 每次 try_acquire 委托前调用的函数，None 时直接委托。
        _lock: 保护 subscriptions。
    """

    def __init__(self, pool: SlotPool) -> None:
        """包装给定池。"""

        self.pool = pool
        self.subscriptions = 0
        self.acquire_hook: Callable[[], None] | None = None
        self._lock = Lock()

    @property
    def size(self) -> int:
        """委托池容量。"""

        return self.pool.size

    @property
    def available(self) -> int:
        """委托池空闲数量。"""

        return self.pool.available

    def try_acquire(self) -> SlotLease | None:
        """先调用注入行为，再委托申请。"""

        if self.acquire_hook is not None:
            self.acquire_hook()
        return self.pool.try_acquire()

    def acquire(self, timeout: float | None = None) -> SlotLease:
        """委托阻塞申请。"""

        return self.pool.acquire(timeout)

    def subscribe_available(self, listener: Callable[[], None]) -> Callable[[], None]:
        """计数订阅，返回同时递减计数的幂等注销函数。"""

        detach = self.pool.subscribe_available(listener)
        with self._lock:
            self.subscriptions += 1
        detached = False

        def unsubscribe() -> None:
            """注销本次订阅并递减计数；重复调用无作用。"""

            nonlocal detached
            with self._lock:
                if detached:
                    return
                detached = True
                self.subscriptions -= 1
            detach()

        return unsubscribe

    def close(self) -> None:
        """委托关闭。"""

        self.pool.close()


def direct_runtime(workers: int | None = None) -> Runtime:
    """创建可限定直接执行线程数的 Runtime。"""

    return Runtime(plugins=(LocalRuntimePlugin(direct_workers=workers),))


@pytest.mark.parametrize("entry", ["start", "run", "iter", "aiter", "arun"])
def test_direct_execution_runs_in_the_provided_slot(entry: str) -> None:
    """每个直接执行入口都在提供方的 Slot 内运行，结束后 Slot 归还。

    Args:
        entry: 被验证的 Runtime 直接执行入口名。
    """

    pool = SlotPool(slots=[Slot(id="caller")])
    with Runtime() as runtime:
        runtime.register("read", Graph(entrypoint="node").add(node=ReadSlot()))
        if entry == "start":
            outputs = runtime.start("read", slots=pool).result(5)
        elif entry == "run":
            outputs = runtime.run("read", slots=pool)
        elif entry == "iter":
            outputs = tuple(runtime.iter("read", slots=pool))
        elif entry == "aiter":

            async def collect() -> tuple[Output, ...]:
                """异步读取全部输出。"""

                return tuple([item async for item in runtime.aiter("read", slots=pool)])

            outputs = asyncio.run(collect())
        else:
            outputs = asyncio.run(runtime.arun("read", slots=pool))
        assert outputs == (Output("caller"),)
        runtime.wait_idle(5)
        assert pool.available == 1
    pool.close()


def test_direct_executions_sharing_one_slot_do_not_overlap() -> None:
    """容量为 1 的池使两次直接执行串行，后者在前者结束前保持 PENDING。"""

    node = Hold()
    pool = SlotPool(1)
    with Runtime() as runtime:
        runtime.register("hold", Graph(entrypoint="node").add(node=node))
        first = runtime.start("hold", slots=pool)
        assert node.entered.wait(5)
        second = runtime.start("hold", slots=pool)
        assert second.status is Execution.Status.PENDING
        assert first.status is Execution.Status.RUNNING
        node.release.set()
        assert first.result(5) == second.result(5)
        assert node.overlap == 1
        runtime.wait_idle(5)
        assert pool.available == 1
    pool.close()


class Echo(Node):
    """原样输出输入的节点。"""

    outputs = {"default": str}

    def execute(self, inputs: Any, context: Any) -> Output:
        """输出 default 输入。"""

        del context
        return Output(inputs["default"])


def test_waiting_for_a_slot_does_not_occupy_a_direct_thread() -> None:
    """单个直接执行线程下，等待 Slot 的执行不阻塞不需要 Slot 的执行。"""

    pool = SlotPool(slots=[Slot(id="only")])
    held = pool.acquire()
    with direct_runtime(workers=1) as runtime:
        runtime.register("read", Graph(entrypoint="node").add(node=ReadSlot()))
        runtime.register("plain", Graph(entrypoint="node").add(node=Echo()))
        waiting = runtime.start("read", slots=pool)
        assert runtime.run("plain", "free", timeout=5) == (Output("free"),)
        assert waiting.status is Execution.Status.PENDING
        held.release()
        assert waiting.result(5) == (Output("only"),)
    pool.close()


def test_cancel_while_waiting_finishes_cancelled_and_detaches() -> None:
    """等待中取消立即结束为 CANCELLED，注销可用通知，Runtime 随即空闲且不泄漏 Slot。"""

    provider = CountingProvider(SlotPool(1))
    held = provider.pool.acquire()
    with Runtime() as runtime:
        runtime.register("read", Graph(entrypoint="node").add(node=ReadSlot()))
        waiting = runtime.start("read", slots=provider)
        assert provider.subscriptions == 1
        assert waiting.cancel()
        assert waiting.status is Execution.Status.CANCELLED
        assert provider.subscriptions == 0
        runtime.wait_idle(5)
        held.release()
        assert provider.available == 1
    provider.close()


def test_cancel_racing_with_lease_arrival_releases_the_lease() -> None:
    """取消在取得 lease 的同时到达时执行保持 CANCELLED，Node 不运行，lease 被释放。"""

    provider = CountingProvider(SlotPool(1))
    held = provider.pool.acquire()
    ran = ThreadEvent()

    class Mark(Node):
        """记录被执行的节点。"""

        outputs = {}

        def execute(self, inputs: Any, context: Any) -> None:
            """置位执行标记。"""

            del inputs, context
            ran.set()

    with Runtime() as runtime:
        runtime.register("mark", Graph(entrypoint="node").add(node=Mark()))
        waiting = runtime.start("mark", slots=provider)
        cancelled: list[Thread] = []

        def cancel_concurrently() -> None:
            """在申请返回 lease 之前于另一线程取消执行，并等待取消已生效。"""

            provider.acquire_hook = None
            thread = Thread(target=waiting.cancel)
            thread.start()
            cancelled.append(thread)
            assert waiting.wait(5)

        provider.acquire_hook = cancel_concurrently
        held.release()
        cancelled[0].join(5)
        assert waiting.status is Execution.Status.CANCELLED
        runtime.wait_idle(5)
        assert not ran.is_set()
        assert provider.available == 1
        assert provider.subscriptions == 0
    provider.close()


def test_closed_provider_fails_the_execution() -> None:
    """已关闭或等待期间关闭的提供方以 SlotProviderClosedError 结束执行，排空不被阻塞。"""

    closed = SlotPool(1)
    closed.close()
    pool = SlotPool(1)
    held = pool.acquire()
    with Runtime() as runtime:
        runtime.register("read", Graph(entrypoint="node").add(node=ReadSlot()))
        rejected = runtime.start("read", slots=closed)
        with pytest.raises(SlotProviderClosedError):
            rejected.result(5)
        assert rejected.status is Execution.Status.FAILED
        waiting = runtime.start("read", slots=pool)
        assert waiting.status is Execution.Status.PENDING
        pool.close()
        with pytest.raises(SlotProviderClosedError):
            waiting.result(5)
        assert waiting.status is Execution.Status.FAILED
        runtime.wait_idle(5)
    held.release()
    assert pool.available == 1


def test_acquire_failure_fails_the_execution_without_retry() -> None:
    """其他申请异常同样结束执行：直接执行没有重投递。"""

    provider = CountingProvider(SlotPool(1))

    def broken() -> None:
        """模拟暂时性申请失败。"""

        raise ConnectionError("slot backend unavailable")

    provider.acquire_hook = broken
    with Runtime() as runtime:
        runtime.register("read", Graph(entrypoint="node").add(node=ReadSlot()))
        execution = runtime.start("read", slots=provider)
        with pytest.raises(ConnectionError, match="unavailable"):
            execution.result(5)
        assert execution.status is Execution.Status.FAILED
        assert provider.subscriptions == 0
        runtime.wait_idle(5)
    assert provider.available == 1
    provider.close()


def test_emit_from_slot_execution_continues_the_slot_chain() -> None:
    """直接执行发出的事件让本地路由 Work 延续同一 Slot，整条链结束后才归还。"""

    seen: list[tuple[str, Slot]] = []

    class Source(Node):
        """写入 Slot 并发出 next 事件。"""

        outputs = {}

        def execute(self, inputs: Any, context: Any) -> None:
            """记录 Slot 并发布事件。"""

            assert context.slot is not None
            context.slot["value"] = inputs["default"]
            seen.append(("source", context.slot))
            context.emit("next")

    class Target(Node):
        """读取源执行写入 Slot 的值。"""

        outputs = {"default": str}

        def execute(self, inputs: Any, context: Any) -> Output:
            """记录 Slot 并输出其中的值。"""

            del inputs
            assert context.slot is not None
            seen.append(("target", context.slot))
            return Output(context.slot["value"])

    pool = SlotPool(1)
    queue_slots = SlotPool(1)
    with Runtime() as runtime:
        runtime.register("source", Graph(entrypoint="node").add(node=Source()))
        runtime.register("target", Graph(entrypoint="node").add(node=Target()))
        runtime.on("next", graph="target", queue="target", slots=queue_slots)
        runtime.start("source", "carried", slots=pool).result(5)
        runtime.wait_idle(5)
        targets = [item for item in runtime.executions() if item.graph == "target"]
        assert [item.result(0) for item in targets] == [(Output("carried"),)]
        assert pool.available == 1
        assert queue_slots.available == 1
    assert [name for name, _ in seen] == ["source", "target"]
    assert seen[0][1] is seen[1][1]
    pool.close()


def test_close_drains_executions_waiting_for_a_slot() -> None:
    """关闭 Runtime 时等待 Slot 的直接执行计为已接受工作，取得 Slot 后完成。"""

    pool = SlotPool(1)
    held = pool.acquire()
    closing = ThreadEvent()

    def observe(event: RuntimeEvent) -> None:
        """在 Runtime 进入 CLOSING 时置位。"""

        if event.kind is RuntimeEventKind.RUNTIME_CLOSING:
            closing.set()

    runtime = Runtime()
    runtime.hooks.on(RUNTIME_EVENT, observe)
    runtime.register("read", Graph(entrypoint="node").add(node=ReadSlot()))
    waiting = runtime.start("read", slots=pool)
    closer = Thread(target=runtime.close)
    closer.start()
    assert closing.wait(5)
    assert closer.is_alive()
    held.release()
    closer.join(5)
    assert not closer.is_alive()
    assert waiting.status is Execution.Status.SUCCEEDED
    assert pool.available == 1
    pool.close()


def test_slots_must_implement_slot_provider() -> None:
    """slots 不是 SlotProvider 时提交前失败，不登记执行。"""

    with Runtime() as runtime:
        runtime.register("read", Graph(entrypoint="node").add(node=ReadSlot()))
        with pytest.raises(TypeError, match="SlotProvider"):
            runtime.start("read", slots=1)  # type: ignore[arg-type]
        assert runtime.executions() == ()
