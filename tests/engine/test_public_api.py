"""精简公共 API 和 Event 值对象测试。"""

from __future__ import annotations

import pickle
from collections.abc import Mapping
from dataclasses import fields
from typing import TYPE_CHECKING, Any

import pytest

import interlace
from interlace import Event
from interlace import adapters, engine, nodes, plugins, runtime, spi
from interlace.engine import errors
from interlace.spi import Work

if TYPE_CHECKING:
    from typing_extensions import assert_type

    class _TypedCoroutineNode(interlace.Node):
        """验证统一基类允许协程方法覆盖且保留具体结果类型。"""

        async def execute(
            self, inputs: Mapping[str, Any], context: interlace.Context
        ) -> interlace.Output:
            """通过带精确返回注解的协程方法满足 Node 契约。

            Args:
                inputs: 当前节点按端口组织的输入。
                context: 当前执行上下文，本静态检查不使用。

            Returns:
                当前 default 输入的 Output 封装。
            """

            del context
            return interlace.Output(inputs["default"])

    async def _check_node_coroutine_override(
        node: _TypedCoroutineNode,
        inputs: Mapping[str, Any],
        context: interlace.Context,
    ) -> None:
        """静态检查协程 Node 覆盖方法的具体输出不会退化为 Any。

        Args:
            node: 使用 async def execute 的统一 Node 子类。
            inputs: 当前节点的输入映射。
            context: 当前执行上下文。
        """

        assert_type(await node.execute(inputs, context), interlace.Output)

    async def _check_execution_await_type(execution: interlace.Execution) -> None:
        """静态检查直接等待 Execution 保留完整输出类型。

        Args:
            execution: 提供同步与异步统一结果契约的执行句柄。
        """

        assert_type(await execution, tuple[interlace.Output, ...])

    def _check_runtime_context_type() -> None:
        """静态检查推荐的上下文管理用法保留 Runtime 与结果类型。"""

        with interlace.Runtime() as instance:
            assert_type(instance, interlace.Runtime)
            assert_type(instance.run("example"), tuple[interlace.Output, ...])


def test_top_level_api_contains_only_core_vocabulary() -> None:
    """顶层只保留写业务与编排所需的六个名字。"""

    assert interlace.__all__ == [
        "Context",
        "Event",
        "Graph",
        "Node",
        "Output",
        "Runtime",
    ]


def test_advanced_packages_expose_explicit_architecture_boundaries() -> None:
    """内核第二层、编排、SPI、插件和默认适配器各自位于独立包。"""

    assert engine.__all__ == [
        "Context",
        "Edge",
        "Event",
        "Execution",
        "ExecutionPlan",
        "Graph",
        "InputPolicy",
        "Node",
        "Output",
        "Slot",
        "SlotPool",
    ]
    assert runtime.__all__ == [
        "EventRouter",
        "GraphWorker",
        "LocalInfrastructurePlugin",
        "LocalRuntimePlugin",
        "Runtime",
    ]
    assert adapters.__all__ == ["memory"]
    assert {"EventBus", "ExecutionLimits", "InputSelector"} <= set(spi.__all__)
    assert {"PluginHost", "ExtensionAPI", "HookBus", "plugin"} <= set(plugins.__all__)
    assert not {"FilterPoint", "RegistryKind"} & set(plugins.__all__)
    assert nodes.__all__ == [
        "KeyedJoin",
        "KeyedJoinOverflowError",
        "KeyedPair",
        "KeyedValue",
    ]
    assert not hasattr(engine, "Runtime")
    assert not hasattr(engine, "Ports")
    assert not hasattr(engine, "PluginHost")
    assert adapters.memory.EventBus.__name__ == "EventBus"
    assert adapters.memory.TaskBackend.__name__ == "TaskBackend"


def test_frozen_ports_are_ordered_snapshots_of_node_declarations() -> None:
    """冻结快照保持端口声明顺序且只读；冻结后修改节点的声明字典不影响已冻结的 Graph。"""

    class Pair(interlace.Node):
        """声明两个输入端口的节点。"""

        inputs = {"left": str, "right": int}

        def execute(self, inputs: Any, context: Any) -> None:
            """不产生输出。"""

    node = Pair()
    graph = interlace.Graph(entrypoint="pair").add(pair=node).freeze()
    node.inputs["extra"] = object
    ports = graph.spec_for("pair").input_ports

    assert tuple(ports) == ("left", "right")
    with pytest.raises(TypeError):
        ports["left"] = object  # type: ignore[index]


def test_event_is_minimal_domain_message() -> None:
    """Event 只保留路由类型和领域 payload。"""

    event = Event(
        "crawl.page.requested",
        {"url": "https://example.com"},
    )

    assert event.type == "crawl.page.requested"
    assert event.payload["url"] == "https://example.com"
    assert tuple(field.name for field in fields(Event)) == ("type", "payload")


def test_work_contains_only_transportable_execution_data() -> None:
    """验证 Work 只携带可传输的执行数据。"""

    Work("crawl.graph", {"url": "https://example.com"})

    assert tuple(field.name for field in fields(Work)) == (
        "graph",
        "inputs",
        "trigger",
        "id",
        "limits",
        "options",
    )


def _error_classes() -> list[type[errors.InterlaceError]]:
    """收集 engine.errors 与官方 Node 中定义的全部公共错误类型。"""

    candidates = [*vars(errors).values(), nodes.KeyedJoinOverflowError]
    return [
        value
        for value in candidates
        if isinstance(value, type) and issubclass(value, errors.InterlaceError)
    ]


@pytest.mark.parametrize("error_type", _error_classes(), ids=lambda cls: cls.__name__)
def test_public_errors_round_trip_through_pickle(error_type) -> None:
    """公共错误跨进程传递时保留类型、消息与附加字段。

    Args:
        error_type: 待检查的公共错误类型。
    """

    event = Event("example.failed", {"id": 1})
    if issubclass(error_type, errors.EventDispatchError):
        error = error_type(event, ValueError("boom"))
    elif issubclass(error_type, errors.ExecutionError):
        error = error_type("failed", graph="flow", node="step", event=event)
    else:
        error = error_type("failed")

    restored = pickle.loads(pickle.dumps(error))

    assert type(restored) is error_type
    assert str(restored) == str(error)
    if isinstance(error, errors.EventDispatchError):
        assert restored.event == event
        assert type(restored.cause) is ValueError
        assert restored.cause.args == ("boom",)
    if isinstance(error, errors.ExecutionError):
        assert (restored.graph, restored.node, restored.event) == (
            "flow",
            "step",
            event,
        )
