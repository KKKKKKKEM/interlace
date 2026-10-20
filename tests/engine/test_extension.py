"""钩子总线的登记身份、组合模式、作用域、快照与具名值契约测试。"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from interlace.engine.errors import ExecutionCancelledError
from interlace.plugins import HookBus, NoticePoint, WrapPoint

NOTICE: NoticePoint[int] = NoticePoint("test/notice")
TYPED: WrapPoint[int, int] = WrapPoint("test/typed", scope=("site",), value_type=int)
WRAP: WrapPoint[int, int] = WrapPoint("test/wrap")


def test_duplicate_registrations_detach_independently() -> None:
    """同一回调的重复登记各自生效；旧句柄重复注销不影响后来的登记。"""

    bus = HookBus()
    seen: list[int] = []
    first = bus.on(NOTICE, seen.append)
    second = bus.on(NOTICE, seen.append)
    bus.notify(NOTICE, 1)
    first()
    first.detach()
    bus.notify(NOTICE, 2)
    second.detach()
    third = bus.on(NOTICE, seen.append)
    second.detach()
    bus.notify(NOTICE, 3)
    assert seen == [1, 1, 2, 3]
    assert not first.active and third.active


def test_wrap_rewrites_by_scope_and_checks_value_type() -> None:
    """中间件按登记顺序改写交给内层的值，只调用作用域匹配者；到达内层的值必须满足 value_type。"""

    bus = HookBus()
    bus.on(TYPED, lambda value, call_next: call_next(value + 1))
    bus.on(TYPED, lambda value, call_next: call_next(value * 10), site="a")
    assert bus.wrap(TYPED, 1, lambda value: value, site="a") == 20
    assert bus.wrap(TYPED, 1, lambda value: value, site="b") == 2
    bus.on(TYPED, lambda value, call_next: call_next(str(value)))
    with pytest.raises(TypeError, match="expected"):
        bus.wrap(TYPED, 1, lambda value: value, site="b")


def test_scope_keys_are_validated_on_registration_and_call() -> None:
    """登记未声明的作用域键或调用缺少声明的键都会明确失败。"""

    bus = HookBus()
    with pytest.raises(ValueError, match="no scope keys"):
        bus.on(TYPED, lambda value, call_next: call_next(value), region="x")
    with pytest.raises(ValueError, match="requires scope keys"):
        bus.wrap(TYPED, 1, lambda value: value)


def test_wrap_nests_outer_first_and_supports_short_circuit_and_retry() -> None:
    """先登记者位于外层；中间件可以不调用 call_next 短路，也可以多次调用重试。"""

    bus = HookBus()
    order: list[str] = []
    attempts: list[int] = []

    def outer(value: int, call_next: Any) -> int:
        """失败时重试一次内层。"""

        order.append("outer")
        try:
            return call_next(value)
        except ValueError:
            return call_next(value)

    def inner(value: int, call_next: Any) -> int:
        """转换输入后调用内层。"""

        order.append("inner")
        return call_next(value + 1)

    def flaky(value: int) -> int:
        """首次失败，之后返回结果。"""

        attempts.append(value)
        if len(attempts) == 1:
            raise ValueError("first attempt")
        return value * 100

    bus.on(WRAP, outer)
    bus.on(WRAP, inner)
    assert bus.wrap(WRAP, 1, flaky) == 200
    assert order == ["outer", "inner", "inner"]
    assert attempts == [2, 2]

    short = HookBus()
    short.on(WRAP, lambda value, call_next: -1)
    assert short.wrap(WRAP, 1, flaky) == -1


def test_wrap_cannot_swallow_control_errors() -> None:
    """中间件吞掉或替换 call_next 抛出的控制异常时宿主重新抛出原异常；普通异常可以恢复。"""

    bus = HookBus()

    def swallow(value: int, call_next: Any) -> int:
        """把内层全部普通异常转换为替代结果。"""

        try:
            return call_next(value)
        except Exception:
            return 0

    def replace(value: int, call_next: Any) -> int:
        """把内层异常替换为另一个异常。"""

        try:
            return call_next(value)
        except Exception as error:
            raise LookupError("replaced") from error

    def cancelled(value: int) -> int:
        """模拟协作式取消。"""

        raise ExecutionCancelledError("cancelled")

    def failed(value: int) -> int:
        """模拟业务失败。"""

        raise ValueError("business")

    bus.on(WRAP, swallow)
    with pytest.raises(ExecutionCancelledError):
        bus.wrap(WRAP, 1, cancelled)
    assert bus.wrap(WRAP, 1, failed) == 0
    replacing = HookBus()
    replacing.on(WRAP, replace)
    with pytest.raises(ExecutionCancelledError):
        replacing.wrap(WRAP, 1, cancelled)


def test_calls_use_the_snapshot_taken_when_they_start() -> None:
    """调用进行中登记或注销的处理函数只影响之后的调用。"""

    bus = HookBus()
    seen: list[str] = []

    def register_more(value: int, call_next: Any) -> int:
        """在调用期间登记另一个中间件并注销自身。"""

        seen.append("first")
        bus.on(WRAP, lambda item, inner: seen.append("late") or inner(item))
        registration.detach()
        return call_next(value)

    registration = bus.on(WRAP, register_more)
    assert bus.wrap(WRAP, 1, lambda value: value) == 1
    assert seen == ["first"]
    assert bus.wrap(WRAP, 2, lambda value: value) == 2
    assert seen == ["first", "late"]


def test_notice_failures_are_isolated_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Notice 处理函数失败被记录，不影响其他处理函数与调用方。

    Args:
        caplog: 日志捕获夹具。
    """

    bus = HookBus()
    seen: list[int] = []

    def broken(value: int) -> None:
        """总是失败。"""

        raise RuntimeError("observer broken")

    bus.on(NOTICE, broken)
    bus.on(NOTICE, seen.append)
    with caplog.at_level(logging.ERROR, logger="interlace.engine.extension"):
        bus.notify(NOTICE, 7)
    assert seen == [7]
    assert [
        str(record.exc_info[1]) for record in caplog.records if record.exc_info
    ] == ["observer broken"]


def test_sync_entries_reject_async_handlers_and_async_entries_await_them() -> None:
    """同步入口拒绝返回 awaitable 的处理函数，异步入口同时接受同步与异步处理函数。"""

    async def double(value: int, call_next: Any) -> int:
        """异步中间件。"""

        return await call_next(value) * 2

    bus = HookBus()
    bus.on(WRAP, double)
    with pytest.raises(TypeError, match="synchronous"):
        bus.wrap(WRAP, 1, lambda value: value)
    bus.on(WRAP, lambda value, call_next: call_next(value + 1))

    async def scenario() -> int:
        """调用异步入口。"""

        async def call(value: int) -> int:
            """异步被包裹操作。"""

            return value + 100

        return await bus.awrap(WRAP, 1, call)

    assert asyncio.run(scenario()) == 204


def test_provided_names_are_unique_and_snapshots_are_stable() -> None:
    """具名值名称唯一，撤销后可重新提供；旧句柄撤销不影响新值；读取结果是不随后续变化的快照。"""

    bus = HookBus()
    first = bus.provide("acme/one", 1)
    with pytest.raises(ValueError, match="already provided"):
        bus.provide("acme/one", 2)
    with pytest.raises(ValueError, match="namespaced"):
        bus.provide("one", 2)
    snapshot = bus.provided()
    first.detach()
    with pytest.raises(LookupError, match="not provided"):
        bus.lookup("acme/one")
    second = bus.provide("acme/one", 3)
    first.detach()
    assert dict(snapshot) == {"acme/one": 1}
    assert bus.lookup("acme/one") == 3 and second.active


def test_detach_owner_removes_only_that_owner() -> None:
    """按登记方批量注销只影响该插件的钩子与具名值。"""

    bus = HookBus()
    seen: list[str] = []
    bus.on(NOTICE, lambda value: seen.append("a"), owner="acme/a")
    bus.on(NOTICE, lambda value: seen.append("b"), owner="acme/b")
    bus.provide("acme/a", 1, owner="acme/a")
    bus.provide("acme/b", 2, owner="acme/b")
    bus.detach_owner("acme/a")
    bus.notify(NOTICE, 0)
    assert seen == ["b"]
    assert dict(bus.provided()) == {"acme/b": 2}


def test_required_wrap_rejects_calls_without_matching_handler() -> None:
    """required 点在调用作用域内没有生效的处理函数时拒绝调用且不执行内层操作，有处理函数时照常包裹。"""

    required: WrapPoint[int, int] = WrapPoint(
        "test/required", scope=("site",), required=True
    )
    bus = HookBus()
    calls: list[int] = []

    def inner(value: int) -> int:
        """记录内层调用。"""

        calls.append(value)
        return value

    with pytest.raises(LookupError, match="test/required"):
        bus.wrap(required, 1, inner, site="a")
    with pytest.raises(LookupError, match="test/required"):
        asyncio.run(bus.awrap(required, 1, inner, site="a"))
    registration = bus.on(
        required, lambda value, call_next: call_next(value + 1), site="b"
    )
    with pytest.raises(LookupError):
        bus.wrap(required, 1, inner, site="a")
    assert bus.wrap(required, 1, inner, site="b") == 2
    registration.detach()
    with pytest.raises(LookupError):
        bus.wrap(required, 1, inner, site="b")
    assert calls == [2]
    assert bus.wrap(WRAP, 5, inner) == 5

    with pytest.raises(TypeError, match="bool"):
        WrapPoint("test/bad", required=1)  # type: ignore[arg-type]
