"""Runtime 边界钩子点与领域钩子点的契约测试。"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from interlace import Event, Graph, Node, Output, Runtime
from interlace.engine.errors import EventDispatchError, InterlaceRuntimeError
from interlace.plugins import WrapPoint
from interlace.plugins.points import (
    EVENT_EMIT,
    EXECUTION_RUN,
    RUNTIME_EVENT,
    WORK_DELIVER,
    WORK_ROUTE,
    ExecutionCall,
)
from interlace.spi import DeliveryOutcome, DeliveryResult

FETCH: WrapPoint[str, str] = WrapPoint("example/fetch", scope=("site",))


class Record(Node):
    """记录输入与执行配置的节点。

    Attributes:
        outputs: 输出收到的值与配置标签。
    """

    outputs = {"value": object}

    def execute(self, inputs, context):
        """输出输入值与 options 中的 tag。"""

        return Output((inputs["default"], context.options.get("tag")), "value")


class Flaky(Node):
    """前两次执行失败的节点。

    Attributes:
        attempts: 已执行次数。
    """

    attempts = 0

    def execute(self, inputs, context):
        """前两次抛出业务异常。"""

        type(self).attempts += 1
        if type(self).attempts < 3:
            raise ConnectionError("temporary")


def test_event_emit_can_rewrite_suppress_or_reject_events() -> None:
    """EVENT_EMIT 中间件可以改写事件决定后续路由、短路不分发，抛出异常时 emit 以 EventDispatchError 失败。"""

    seen: list[Event] = []

    def rewrite(event: Event, call_next: Any) -> Event:
        """把 draft 改写为 ready，抑制 muted，拒绝 forbidden。"""

        if event.payload == "forbidden":
            raise PermissionError("forbidden")
        if event.payload == "muted":
            return event
        return call_next(Event("ready", event.payload))

    with Runtime() as runtime:
        runtime.hooks.on(EVENT_EMIT, rewrite, event_type="draft")
        runtime.observe("ready", seen.append)
        runtime.observe("draft", seen.append)
        assert runtime.emit("draft", 1) == Event("ready", 1)
        assert runtime.emit("draft", "muted") == Event("draft", "muted")
        with pytest.raises(EventDispatchError) as caught:
            runtime.emit("draft", "forbidden")
        assert isinstance(caught.value.__cause__, PermissionError)
        runtime.emit("other", 2)
    assert seen == [Event("ready", 1)]


def test_work_route_sets_stable_identity_and_options() -> None:
    """WORK_ROUTE 中间件可以为路由产生的 Work 设置稳定 ID 与执行配置，传入非 Work 时拒绝提交。"""

    with Runtime() as runtime:
        runtime.register("record", Graph(entrypoint="node").add(node=Record()))
        runtime.on("go", graph="record", queue="record")
        runtime.hooks.on(
            WORK_ROUTE,
            lambda work, call_next: call_next(
                replace(work, id=f"page-{work.inputs}", options={"tag": "X"})
            ),
            queue="record",
        )
        runtime.emit("go", 7)
        runtime.wait_idle(5)
        assert runtime.get_execution("page-7").result() == (Output((7, "X"), "value"),)
        with (
            runtime.hooks.on(
                WORK_ROUTE,
                lambda work, call_next: call_next(work.inputs),
                queue="record",
            ),
            pytest.raises(EventDispatchError) as caught,
        ):
            runtime.emit("go", 8)
        assert isinstance(caught.value.__cause__, TypeError)


def test_work_deliver_can_turn_failures_into_bounded_retries() -> None:
    """WORK_DELIVER 中间件把失败改写为 RETRY 后由任务后端重投，结果写入生命周期事实。"""

    Flaky.attempts = 0
    outcomes: list[str | None] = []

    def retry(delivery: Any, call_next: Any) -> DeliveryResult:
        """前两次尝试失败时请求重投。"""

        result = call_next(delivery)
        if result.outcome is DeliveryOutcome.REJECT and delivery.attempt < 3:
            return DeliveryResult.retry(result.error)
        return result

    with Runtime() as runtime:
        runtime.register("flaky", Graph(entrypoint="node").add(node=Flaky()))
        runtime.on("go", graph="flaky", queue="flaky")
        runtime.hooks.on(WORK_DELIVER, retry, queue="flaky")
        runtime.hooks.on(
            RUNTIME_EVENT,
            lambda event: outcomes.append(event.status),
            kind="work.finished",
        )
        runtime.emit("go")
        runtime.wait_idle(5)
    assert Flaky.attempts == 3
    assert outcomes == ["retried", "retried", "acked"]


def test_work_deliver_failure_rejects_the_delivery() -> None:
    """WORK_DELIVER 中间件抛出异常或返回错误类型时交付被拒绝，并经 wait_idle 传播。"""

    with Runtime() as runtime:
        runtime.register("record", Graph(entrypoint="node").add(node=Record()))
        runtime.on("go", graph="record", queue="record")
        runtime.hooks.on(WORK_DELIVER, lambda delivery, call_next: "wrong")
        runtime.emit("go", 1)
        with pytest.raises(TypeError, match="DeliveryResult"):
            runtime.wait_idle(5)


def test_execution_run_may_replace_only_inputs_and_options() -> None:
    """EXECUTION_RUN 可替换输入与配置；替换执行身份时执行失败。"""

    with Runtime() as runtime:
        runtime.register("record", Graph(entrypoint="node").add(node=Record()))
        registration = runtime.hooks.on(
            EXECUTION_RUN,
            lambda call, call_next: call_next(
                replace(call, inputs=call.inputs * 2, options={"tag": "run"})
            ),
            graph="record",
        )
        assert runtime.run("record", 3) == (Output((6, "run"), "value"),)
        registration.detach()

        def foreign(call: ExecutionCall, call_next: Any) -> None:
            """尝试替换 Graph。"""

            return call_next(replace(call, graph=Graph(entrypoint="x").add(x=Record())))

        runtime.hooks.on(EXECUTION_RUN, foreign, graph="record")
        with pytest.raises(InterlaceRuntimeError, match="only replace inputs"):
            runtime.run("record", 3)


def test_nodes_trigger_plugin_defined_points_through_context() -> None:
    """节点经 Context.hooks 触发插件定义的钩子点；作用域决定生效的中间件。"""

    class Fetch(Node):
        """经 FETCH 钩子点下载的节点。

        Attributes:
            outputs: 输出下载结果。
        """

        outputs = {"body": str}

        def execute(self, inputs, context):
            """以 site=a 触发 FETCH。"""

            body = context.hooks.wrap(
                FETCH, inputs["default"], lambda url: f"<{url}>", site="a"
            )
            return Output(body, "body")

    with Runtime() as runtime:
        runtime.register("fetch", Graph(entrypoint="node").add(node=Fetch()))
        runtime.hooks.on(FETCH, lambda url, call_next: call_next(url.upper()), site="a")
        runtime.hooks.on(FETCH, lambda url, call_next: "blocked", site="b")
        assert runtime.run("fetch", "x") == (Output("<X>", "body"),)
