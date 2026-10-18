"""具名 selector 提供、生命周期观察和官方可复用 Node 的契约测试。"""

from __future__ import annotations

import pytest

from interlace import Graph, Node, Output, Runtime
from interlace.adapters import memory
from interlace.engine.errors import (
    ExecutionError,
    GraphValidationError,
    IncompleteInputsError,
    PolicySelectionError,
)
from interlace.engine.observation import RuntimeEventKind
from interlace.engine import InputPolicy
from interlace.engine.policies import bind_policy
from interlace.nodes import KeyedJoin, KeyedJoinOverflowError, KeyedPair, KeyedValue
from interlace.plugins.points import RUNTIME_EVENT
from interlace.spi import (
    DeliveryResult,
    Work,
)


class Source(Node):
    """当前契约测试使用的 Source 替代实现。

    Attributes:
        inputs: 节点声明的输入端口及其类型。
        outputs: 节点声明的输出端口及其类型。
    """

    inputs = {"value": int}
    outputs = {"value": int}

    def execute(self, inputs, context):
        """把 value 输入原样输出到 value 端口。"""

        del context
        return Output(inputs["value"], "value")


def test_runtime_observer_sees_read_only_lifecycle_and_cannot_break_work(
    caplog,
) -> None:
    """验证生命周期观察只读且观察失败不影响工作结果。

    Args:
        caplog: 当前用例使用的 caplog 夹具或参数化输入。
    """

    events = []

    def observe(event) -> None:
        """注册只读事件观察者，不建立目标 Graph 路由。"""

        events.append(event)
        raise RuntimeError("telemetry failed")

    graph = Graph(entrypoint="source").add(source=Source())
    with Runtime() as runtime:
        handle = runtime.hooks.on(RUNTIME_EVENT, observe)
        runtime.register("observed.graph", graph)
        assert runtime.run("observed.graph", 3) == (Output(3, "value"),)
        handle.detach()

    assert [event.kind for event in events] == [
        RuntimeEventKind.EXECUTION_STARTED,
        RuntimeEventKind.NODE_STARTED,
        RuntimeEventKind.OUTPUT_ROUTED,
        RuntimeEventKind.NODE_FINISHED,
        RuntimeEventKind.EXECUTION_FINISHED,
    ]
    assert events[-1].attributes["steps"] == 1
    logged = [record.exc_info[1] for record in caplog.records if record.exc_info]
    assert logged and all(str(error) == "telemetry failed" for error in logged)
    with pytest.raises(TypeError):
        events[-1].attributes["steps"] = 2


class AnyTwo:
    def select(self, ports, available, config):
        """仅根据端口名称和可用 token 数量选择本次输入组合。

        Args:
            ports: 保持声明顺序的输入端口名称。
            available: 各端口当前可消费的 token 数量。
            config: 当前具名策略的参数映射。

        Returns:
            符合当前可用条件的选择结果，没有可执行输入时不触发。
        """

        count = config["count"]
        selected = tuple(port for port in ports if available[port])
        return selected[:count] if len(selected) >= count else None


class ContributedNode(Node):
    """当前契约测试使用的 ContributedNode 替代实现。

    Attributes:
        inputs: 节点声明的输入端口及其类型。
        outputs: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
    """

    inputs = {"a": int, "b": int, "c": int}
    outputs = {"total": int}
    input_policy = InputPolicy("example.test/any-two", {"count": 2})

    def execute(self, inputs, context):
        """输出全部输入值之和。"""

        del context
        return Output(sum(inputs.values()), "total")


class FanOut(Node):
    """当前契约测试使用的 FanOut 替代实现。

    Attributes:
        inputs: 节点声明的输入端口及其类型。
        outputs: 节点声明的输出端口及其类型。
    """

    inputs = {"value": int}
    outputs = {"a": int, "b": int}

    def execute(self, inputs, context):
        """把输入值发往 a 端口，把加一后的值发往 b 端口。"""

        del context
        return Output(inputs["value"], "a"), Output(inputs["value"] + 1, "b")


def test_contributed_policy_is_bound_when_graph_freezes() -> None:
    """验证 Graph 冻结时按名称固定已提供 selector 的实现快照。"""

    graph = (
        Graph(entrypoint="source")
        .add(source=FanOut(), join=ContributedNode())
        .connect("source", "join", source_port="a", target_port="a")
        .connect("source", "join", source_port="b", target_port="b")
    )
    with Runtime() as runtime:
        runtime.hooks.provide("example.test/any-two", AnyTwo())
        runtime.register("policy.graph", graph)
        assert runtime.run("policy.graph", 2) == (Output(5, "total"),)


def test_policy_config_is_deeply_frozen_before_graph_registration() -> None:
    """调用方与 selector 都不能改写冻结策略的嵌套配置。"""

    original = {"ports": ["a", "b"], "limits": {"count": 2}}
    policy = InputPolicy("example.test/frozen", original)
    original["ports"].append("c")
    original["limits"]["count"] = 3

    assert policy.config["ports"] == ("a", "b")
    assert policy.config["limits"]["count"] == 2
    with pytest.raises(TypeError):
        policy.config["limits"]["count"] = 4


def test_missing_contributed_policy_fails_at_registration() -> None:
    """验证缺少贡献策略时注册 Graph 明确失败。"""

    graph = Graph(entrypoint="join").add(join=ContributedNode())
    with (
        Runtime() as runtime,
        pytest.raises(GraphValidationError, match="not provided"),
    ):
        runtime.register("missing.graph", graph)


def test_memory_delivery_retries_and_increments_attempt() -> None:
    """验证内存后端根据交付决定重试并递增尝试次数。"""

    backend = memory.TaskBackend()
    attempts = []

    def handle(delivery):
        """处理测试投递，并返回明确的交付结果。

        Args:
            delivery: 携带尝试次数和可选 Slot lease 的本次投递。

        Returns:
            本次测试投递对应的 DeliveryResult。
        """

        attempts.append(delivery.attempt)
        if delivery.attempt < 3:
            return DeliveryResult.retry()
        return DeliveryResult.ack()

    backend.bind("retry", handle, concurrency=1)
    backend.submit("retry", Work("graph"))
    backend.wait_idle()
    backend.close()

    assert attempts == [1, 2, 3]


def test_memory_delivery_reports_rejection() -> None:
    """验证内存后端向等待方传播投递拒绝。"""

    backend = memory.TaskBackend()
    backend.bind(
        "reject",
        lambda delivery: DeliveryResult.reject(ValueError(delivery.work.id)),
        concurrency=1,
    )
    backend.submit("reject", Work("graph", id="work-1"))
    with pytest.raises(ValueError, match="work-1"):
        backend.wait_idle()
    backend.close()


def test_memory_delivery_stops_after_configured_attempts() -> None:
    """验证内存重投递在配置次数耗尽后停止。"""

    backend = memory.TaskBackend(max_delivery_attempts=2)
    attempts = []

    def retry(delivery):
        """创建请求后端重新投递的交付决定。

        Args:
            delivery: 携带尝试次数和可选 Slot lease 的本次投递。

        Returns:
            当前测试回调收集或构造的结果。
        """

        attempts.append(delivery.attempt)
        return DeliveryResult.retry()

    backend.bind("retry", retry, concurrency=1)
    backend.submit("retry", Work("graph", id="bounded"))
    with pytest.raises(RuntimeError, match="max_delivery_attempts=2"):
        backend.wait_idle()
    backend.close()
    assert attempts == [1, 2]


class PairSource(Node):
    """当前契约测试使用的 PairSource 替代实现。

    Attributes:
        inputs: 节点声明的输入端口及其类型。
        outputs: 节点声明的输出端口及其类型。
    """

    inputs = {"items": tuple}
    outputs = {"left": KeyedValue, "right": KeyedValue}

    def execute(self, inputs, context):
        """按 (端口, 值) 对把 items 中的每个值发往对应端口。"""

        del context
        return tuple(Output(item, side) for side, item in inputs["items"])


def keyed_graph() -> Graph:
    """构造用于验证按键关联输入的 Graph。

    Returns:
        连接输入分发与按键关联节点的 Graph。
    """

    return (
        Graph(entrypoint="source")
        .add(source=PairSource(), join=KeyedJoin())
        .connect("source", "join", source_port="left", target_port="left")
        .connect("source", "join", source_port="right", target_port="right")
    )


def test_keyed_join_matches_interleaved_values_by_key() -> None:
    """验证交错到达的数据按领域键正确配对。"""

    items = (
        ("left", KeyedValue("a", "A-left")),
        ("left", KeyedValue("b", "B-left")),
        ("right", KeyedValue("b", "B-right")),
        ("right", KeyedValue("a", "A-right")),
    )
    with Runtime() as runtime:
        runtime.register("join.graph", keyed_graph())
        outputs = runtime.run("join.graph", items)

    assert outputs == (
        Output(KeyedPair("b", "B-left", "B-right"), "joined"),
        Output(KeyedPair("a", "A-left", "A-right"), "joined"),
    )


def test_keyed_join_rejects_unmatched_values_at_quiescence() -> None:
    """验证静止时未配对的关联数据导致执行失败。"""

    items = (("left", KeyedValue("a", "only-left")),)
    with Runtime() as runtime:
        runtime.register("join.graph", keyed_graph())
        with pytest.raises(IncompleteInputsError, match="unmatched"):
            runtime.run("join.graph", items)


def test_keyed_join_overflow_raises_interlace_execution_error() -> None:
    """超过 max_pending 时以 Interlace 执行错误失败，而不是内建 OverflowError。"""

    items = (("left", KeyedValue("a", 1)), ("left", KeyedValue("b", 2)))
    graph = (
        Graph(entrypoint="source")
        .add(source=PairSource(), join=KeyedJoin(max_pending=1))
        .connect("source", "join", source_port="left", target_port="left")
        .connect("source", "join", source_port="right", target_port="right")
    )
    with Runtime() as runtime:
        runtime.register("join.graph", graph)
        with pytest.raises(KeyedJoinOverflowError, match="max_pending=1") as caught:
            runtime.run("join.graph", items)

    assert isinstance(caught.value, ExecutionError)
    assert not isinstance(caught.value, OverflowError)


def test_keyed_join_pairs_interleaved_batch_within_pending_limit() -> None:
    """同一 firing 交错产出的左右值及时配对，未配对积压不会因先排空一侧而超限。"""

    items = tuple(
        (side, KeyedValue(index, f"{side}-{index}"))
        for index in range(5)
        for side in ("left", "right")
    )
    graph = (
        Graph(entrypoint="source")
        .add(source=PairSource(), join=KeyedJoin(max_pending=1))
        .connect("source", "join", source_port="left", target_port="left")
        .connect("source", "join", source_port="right", target_port="right")
    )
    with Runtime() as runtime:
        runtime.register("join.graph", graph)
        outputs = runtime.run("join.graph", items)

    assert outputs == tuple(
        Output(KeyedPair(index, f"left-{index}", f"right-{index}"), "joined")
        for index in range(5)
    )


def test_policy_ref_compares_by_value_and_is_explicitly_unhashable() -> None:
    """InputPolicy 按名称与冻结配置比较；只读配置不能稳定哈希，因此显式不可哈希。"""

    first = InputPolicy("example.test/any-two", {"count": 2})

    assert first == InputPolicy("example.test/any-two", {"count": 2})
    assert first != InputPolicy("example.test/any-two", {"count": 3})
    assert InputPolicy.__hash__ is None
    with pytest.raises(TypeError, match="unhashable"):
        hash(first)


class SelectsEmptyPort:
    """违反 selector 契约、总是选择空端口 c 的测试策略。"""

    def select(self, ports, available, config):
        """返回未持有 token 的端口。"""

        del ports, available, config
        return ("c",)


def test_bound_policy_rejects_selector_contract_violation() -> None:
    """BoundPolicy 把非法选择报告为 PolicySelectionError，graph 与 node 留给执行器补齐。"""

    bound = bind_policy(
        InputPolicy("example.test/bad"), {"example.test/bad": SelectsEmptyPort()}
    )

    with pytest.raises(PolicySelectionError, match="invalid selection") as caught:
        bound.select(("a", "b", "c"), {"a": [1], "b": [], "c": []})
    assert caught.value.graph is None and caught.value.node is None


def test_selector_violation_fails_execution_with_policy_selection_error() -> None:
    """运行时 selector 违反契约时，execution 以带 graph 与 node 的 PolicySelectionError 失败。"""

    class BadPolicyNode(ContributedNode):
        """使用违约 selector 的贡献策略节点。"""

        input_policy = InputPolicy("example.test/bad")

    graph = (
        Graph(entrypoint="source")
        .add(source=FanOut(), join=BadPolicyNode())
        .connect("source", "join", source_port="a", target_port="a")
        .connect("source", "join", source_port="b", target_port="b")
    )
    with Runtime() as runtime:
        runtime.hooks.provide("example.test/bad", SelectsEmptyPort())
        runtime.register("bad.policy", graph)
        with pytest.raises(PolicySelectionError) as caught:
            runtime.run("bad.policy", 1)

    assert caught.value.graph == "bad.policy"
    assert caught.value.node == "join"
