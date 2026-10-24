"""由 RUNTIME_EVENT 派生的生命周期指标契约：执行、节点、Work 与事件。"""

from __future__ import annotations

from typing import Any

import pytest

from interlace import Graph, Node, Output, Runtime
from interlace.metrics import Metrics, count


class Source(Node):
    """记录领域计数并把输入加一后传给下游。"""

    def execute(self, inputs: Any, context: Any) -> Output:
        """经上下文钩子视图计数后输出。

        Args:
            inputs: default 端口的整数输入。
            context: 当前执行上下文。

        Returns:
            输入加一。
        """

        count(context.hooks, "demo.items", 2, kind_label="x")
        return Output(inputs["default"] + 1)


class Double(Node):
    """把输入乘以二。"""

    def execute(self, inputs: Any, context: Any) -> Output:
        """返回输入的两倍。

        Args:
            inputs: default 端口的整数输入。
            context: 当前执行上下文。

        Returns:
            输入乘以二。
        """

        del context
        return Output(inputs["default"] * 2)


class Broken(Node):
    """立即以业务异常失败。"""

    def execute(self, inputs: Any, context: Any) -> None:
        """抛出测试异常。

        Args:
            inputs: 忽略的入口值。
            context: 当前执行上下文。

        Raises:
            ValueError: 测试业务失败。
        """

        del inputs, context
        raise ValueError("broken")


class Countdown(Node):
    """沿自环把计数减一，直到零时从 done 端口输出。"""

    outputs = {"again": int, "done": int}

    def execute(self, inputs: Any, context: Any) -> Output:
        """未到零时沿 again 回边继续，否则输出 done。

        Args:
            inputs: default 端口的剩余次数。
            context: 当前执行上下文。

        Returns:
            again 端口的剩余次数减一，或 done 端口的零。
        """

        del context
        value = inputs["default"]
        return Output(value - 1, "again") if value > 0 else Output(value, "done")


def series(metrics: Metrics, name: str) -> list[dict[str, Any]]:
    """读取一个指标名的全部序列。

    Args:
        metrics: 测试聚合器。
        name: 指标名。

    Returns:
        快照中的序列列表；指标不存在时为空列表。
    """

    return metrics.snapshot().get(name, {"series": []})["series"]


def values(metrics: Metrics, name: str) -> dict[tuple[tuple[str, str], ...], Any]:
    """把计数器序列整理为排序标签到值的映射，直方图映射到观测次数。

    Args:
        metrics: 测试聚合器。
        name: 指标名。

    Returns:
        排序后的标签元组到值或观测次数的映射。
    """

    return {
        tuple(sorted(item["labels"].items())): item.get("value", item.get("count"))
        for item in series(metrics, name)
    }


def test_run_records_domain_and_lifecycle_metrics_for_success_and_failure() -> None:
    """成功与失败的直接执行都产生执行、节点计数与耗时，领域计数经同一聚合器记录。"""

    metrics = Metrics()
    with Runtime(plugins=[metrics.plugin]) as runtime:
        runtime.register(
            "demo",
            Graph(entrypoint="source")
            .add(source=Source(), double=Double())
            .connect("source", "double"),
        )
        runtime.register("broken", Graph(entrypoint="broken").add(broken=Broken()))
        assert runtime.run("demo", 1)[0].value == 4
        with pytest.raises(Exception, match="broken"):
            runtime.run("broken", None)

    assert values(metrics, "demo.items") == {(("kind_label", "x"),): 2.0}
    assert values(metrics, "interlace.executions") == {
        (("graph", "demo"), ("status", "succeeded")): 1.0,
        (("graph", "broken"), ("status", "failed")): 1.0,
    }
    assert values(metrics, "interlace.execution.seconds") == {
        (("graph", "demo"), ("status", "succeeded")): 1,
        (("graph", "broken"), ("status", "failed")): 1,
    }
    assert values(metrics, "interlace.node.firings") == {
        (("graph", "demo"), ("node", "source"), ("status", "succeeded")): 1.0,
        (("graph", "demo"), ("node", "double"), ("status", "succeeded")): 1.0,
        (("graph", "broken"), ("node", "broken"), ("status", "failed")): 1.0,
    }
    assert values(metrics, "interlace.node.seconds") == {
        (("graph", "demo"), ("node", "source")): 1,
        (("graph", "demo"), ("node", "double")): 1,
        (("graph", "broken"), ("node", "broken")): 1,
    }
    for item in series(metrics, "interlace.execution.seconds"):
        assert 0.0 <= item["max"] <= item["sum"]
    labels = {
        key
        for name in metrics.snapshot()
        for item in series(metrics, name)
        for key in item["labels"]
    }
    assert labels <= {"graph", "node", "status", "queue", "event_type", "kind_label"}

    text = metrics.prometheus()
    assert 'demo_items_total{kind_label="x"} 2.0' in text
    assert 'interlace_executions_total{graph="demo",status="succeeded"} 1.0' in text
    assert "# TYPE interlace_node_seconds summary" in text


def test_node_timing_pairs_every_firing_of_a_cycle() -> None:
    """自环节点的每次 firing 都与其开始事件配对，计时表在结束后清空。"""

    metrics = Metrics()
    with Runtime(plugins=[metrics.plugin]) as runtime:
        runtime.register(
            "countdown",
            Graph(entrypoint="loop")
            .add(loop=Countdown())
            .connect("loop", "loop", source_port="again"),
        )
        assert runtime.run("countdown", 4)[0].value == 0

    assert values(metrics, "interlace.node.firings") == {
        (("graph", "countdown"), ("node", "loop"), ("status", "succeeded")): 5.0
    }
    assert values(metrics, "interlace.node.seconds") == {
        (("graph", "countdown"), ("node", "loop")): 5
    }
    assert metrics._firings == {}
    assert metrics._executions == {}


def test_queue_route_records_events_and_works() -> None:
    """Runtime.on 与 emit 产生事件、提交与交付结果计数，Work 执行同样计入执行指标。"""

    metrics = Metrics()
    with Runtime(plugins=[metrics.plugin]) as runtime:
        runtime.register("double", Graph(entrypoint="double").add(double=Double()))
        runtime.register("broken", Graph(entrypoint="broken").add(broken=Broken()))
        runtime.on("value", graph="double", queue="values")
        runtime.on("fault", graph="broken", queue="faults")
        runtime.emit("value", 1)
        runtime.emit("value", 2)
        runtime.emit("fault", None)
        with pytest.raises(Exception, match="broken"):
            runtime.wait_idle(2)

    assert values(metrics, "interlace.events") == {
        (("event_type", "value"),): 2.0,
        (("event_type", "fault"),): 1.0,
    }
    assert values(metrics, "interlace.works.submitted") == {
        (("queue", "values"),): 2.0,
        (("queue", "faults"),): 1.0,
    }
    assert values(metrics, "interlace.works") == {
        (("queue", "values"), ("status", "acked")): 2.0,
        (("queue", "faults"), ("status", "rejected")): 1.0,
    }
    assert values(metrics, "interlace.executions") == {
        (("graph", "double"), ("status", "succeeded")): 2.0,
        (("graph", "broken"), ("status", "failed")): 1.0,
    }
