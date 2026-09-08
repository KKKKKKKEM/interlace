"""精简公共 API 和 Event 值对象测试。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields
from typing import TYPE_CHECKING, Any

import pytest

import interlace
from interlace import Event, Ports, adapters, engine, nodes, plugins, runtime, spi
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
    """顶层只增加可控执行句柄，不暴露内部调度与 checkpoint DTO。"""

    assert interlace.__all__ == [
        "Context",
        "Edge",
        "Event",
        "Execution",
        "ExecutionLimits",
        "ExecutionPlan",
        "ExecutionStatus",
        "Graph",
        "InputPolicy",
        "Node",
        "Output",
        "Ports",
        "Runtime",
        "Slot",
        "SlotPool",
    ]


def test_advanced_packages_expose_explicit_architecture_boundaries() -> None:
    """内核、编排、SPI、插件和默认适配器不再聚合到 engine。"""

    assert engine.__all__ == [name for name in interlace.__all__ if name != "Runtime"]
    assert runtime.__all__ == [
        "EventRouter",
        "GraphWorker",
        "LocalRuntimePlugin",
        "Runtime",
    ]
    assert adapters.__all__ == ["memory"]
    assert "EventBus" in spi.__all__
    assert "PluginHost" in plugins.__all__
    assert "ContributionPlugin" in plugins.__all__
    assert nodes.__all__ == ["KeyedJoin", "KeyedPair", "KeyedValue"]
    assert not hasattr(engine, "Runtime")
    assert not hasattr(engine, "PluginHost")
    assert not hasattr(interlace, "AsyncNode")
    assert not hasattr(engine, "AsyncNode")
    assert not hasattr(engine.core, "AsyncNode")
    assert adapters.memory.EventBus.__name__ == "EventBus"
    assert adapters.memory.TaskBackend.__name__ == "TaskBackend"
    assert not hasattr(adapters.memory, "MemoryEventBus")
    assert not hasattr(adapters.memory, "MemoryTaskBackend")
    assert not hasattr(plugins, "ExtensionPlugin")


def test_ports_are_immutable_and_ordered() -> None:
    """Ports 保存声明顺序并拒绝修改。"""

    ports = Ports(left=str, right=int)

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
