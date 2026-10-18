"""Context 执行局部状态、构造边界与 Event 值语义契约测试。"""

from __future__ import annotations

from collections.abc import Callable, MutableMapping
from typing import Any

import pytest

from interlace import Context, Event
from interlace.engine.events import bind_context


def test_context_state_is_shared_by_scope_and_namespace() -> None:
    """验证相同作用域和命名空间共享执行局部状态。"""

    local: dict[tuple[str, str], MutableMapping[str, Any]] = {}
    first = bind_context(lambda event: None, None, local=local, scope="first")
    same = bind_context(lambda event: None, None, local=local, scope="first")
    other = bind_context(lambda event: None, None, local=local, scope="other")

    first.state("example/state")["value"] = 1

    assert same.state("example/state") == {"value": 1}
    assert other.state("example/state") == {}


def test_context_state_does_not_cross_execution_storage() -> None:
    """验证局部状态不会跨越独立执行存储。"""

    first = bind_context(lambda event: None, None, local={}, scope="node")
    second = bind_context(lambda event: None, None, local={}, scope="node")

    first.state("example/state")["value"] = 1

    assert second.state("example/state") == {}


def test_context_scope_and_namespace_cannot_collide_on_separators() -> None:
    """验证作用域名称中的分隔符不会造成状态键碰撞。"""

    local: dict[tuple[str, str], MutableMapping[str, Any]] = {}
    first = bind_context(lambda event: None, None, local=local, scope="a:b")
    other = bind_context(lambda event: None, None, local=local, scope="a")
    unscoped = bind_context(lambda event: None, None, local=local)

    first.state("c")["value"] = 1

    assert other.state("b:c") == {}
    assert unscoped.state("a:b:c") == {}


def test_context_registers_quiescence_callbacks_in_order() -> None:
    """验证静止阶段回调按注册顺序保存。"""

    callbacks = []
    finalizers: list[Callable[[], None]] = []
    context = bind_context(lambda event: None, None, finalizers=finalizers)
    context.on_quiescence(lambda: callbacks.append("first"))
    context.on_quiescence(lambda: callbacks.append("second"))

    for finalizer in finalizers:
        finalizer()

    assert callbacks == ["first", "second"]


def test_public_context_constructor_only_composes_parent() -> None:
    """公开构造只接受 parent；执行绑定只能通过 bind_context 创建。"""

    with pytest.raises(TypeError):
        Context(lambda event: None)  # type: ignore[misc]
    with pytest.raises(TypeError):
        Context()  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="callable"):
        bind_context(None, None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="Slot"):
        bind_context(lambda event: None, object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="control callbacks"):
        bind_context(lambda event: None, None, checkpoint=1)  # type: ignore[arg-type]


def test_proxy_context_exposes_every_capability_without_uninitialized_slots() -> None:
    """组合上下文读取全部能力都委托给基础上下文，不会遇到未初始化字段。"""

    base = bind_context(lambda event: None, None, options={"example.flag": True})
    proxy = Context(parent=base)

    assert proxy.slot is None
    assert proxy.options == {"example.flag": True}
    assert proxy.cancelled is False
    proxy.checkpoint()
    assert proxy.state("example/state") is base.state("example/state")
    assert proxy.emit("example.done") == Event("example.done")


def test_event_uses_value_equality_and_payload_dependent_hash() -> None:
    """Event 按值相等；只有 payload 可哈希时 Event 才可哈希。"""

    assert Event("example.done", {"id": 1}) == Event("example.done", {"id": 1})
    assert hash(Event("example.done", (1, 2))) == hash(Event("example.done", (1, 2)))
    with pytest.raises(TypeError):
        hash(Event("example.done", {"id": 1}))
