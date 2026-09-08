"""实际节点参数、逐项输出和日志归属的服务契约。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("jsonrpcserver")

from interlace import Graph, Node, Output, Ports, Runtime
from interlace.engine.hooks import HookPhase, NodeCall, NodeHook, StopGraph
from interlace.engine.observation import current_node_event
from interlace.service import GraphService

# 通过传播到根处理器的标准日志验证采集，测试不替换全局日志工厂。
_LOGGER = logging.getLogger("interlace.test.node")


class Echo(Node):
    """产出输入字典并记录日志的节点。

    Attributes:
        input_ports: 接收字典对象。
        output_ports: 产出字典对象。
    """

    input_ports = Ports(default=dict)
    output_ports = Ports(default=dict)

    def execute(self, inputs: Any, context: Any) -> Output:
        """记录当前业务标识并直接返回数据。

        Args:
            inputs: 含 default 字典的数据。
            context: 本次执行上下文。

        Returns:
            当前字典的节点原始输出。
        """

        del context
        _LOGGER.warning("同步处理 %s", inputs["default"]["id"])
        return Output(inputs["default"])


class AsyncEcho(Echo):
    """用于验证共享事件循环中的日志隔离。"""

    async def execute(self, inputs: Any, context: Any) -> Output:
        """在异步等待前后发出属于同一次 firing 的日志。

        Args:
            inputs: 含业务标识的字典。
            context: 本次执行上下文。

        Returns:
            带原始业务标识的输出。
        """

        del context
        _LOGGER.warning("异步开始 %s", inputs["default"]["id"])
        await asyncio.sleep(0.03)
        _LOGGER.warning("异步结束 %s", inputs["default"]["id"])
        return Output(inputs["default"])


class Rewrite(NodeHook):
    """在采集器注册之后转换节点输入和输出。"""

    def enter(self, call: NodeCall) -> NodeCall:
        """改写实际节点将收到的输入。

        Args:
            call: 当前调用。

        Returns:
            标识已变更的调用。
        """

        return call.with_inputs({"default": {"id": "rewritten"}})

    def exit(self, call: NodeCall, output: Output) -> Output:
        """修改实际输出对象，检验采集快照是否独立。

        Args:
            call: 当前调用。
            output: 节点原始输出。

        Returns:
            Hook 转换后的终端输出。
        """

        del call
        output.value["id"] = "terminal"
        return output


def test_inspection_records_actual_inputs_before_output_rewrites() -> None:
    """采集位置不受 Hook 注册顺序影响，快照不随原对象变化。"""

    with Runtime() as runtime:
        runtime.register("echo", Graph(entrypoint="echo").add(echo=Echo()))
        service = GraphService(runtime)
        runtime.attach(Rewrite())
        handle = runtime.start(
            "echo", {"id": "original"}, options={"test": {"token": "hidden"}}
        )
        assert handle.result()[0].value == {"id": "terminal"}
        runtime.wait_idle()
        records = service.store.events(handle.id)
        inputs = next(item for item in records if item["kind"] == "node.input")[
            "attributes"
        ]
        outputs = next(item for item in records if item["kind"] == "node.output")[
            "attributes"
        ]
        assert inputs["inputs"] == {"default": {"id": "rewritten"}}
        assert inputs["options"] == {"test": {"token": "[REDACTED]"}}
        assert outputs["output"]["value"] == {"id": "rewritten"}
        assert any(
            item["kind"] == "node.log" and "rewritten" in item["attributes"]["message"]
            for item in records
        )
        assert current_node_event() is None
        service.close()


def test_async_concurrent_executions_do_not_mix_parameters_or_logs() -> None:
    """共享异步线程上的两个执行必须分别保存参数、日志和步骤。"""

    with Runtime() as runtime:
        runtime.register("async", Graph(entrypoint="echo").add(echo=AsyncEcho()))
        service = GraphService(runtime)
        handles = [
            (key, runtime.start("async", {"id": key, "password": "secret"}))
            for key in ("alpha", "beta")
        ]
        for _, handle in handles:
            handle.result()
        runtime.wait_idle()
        for key, handle in handles:
            records = service.store.events(handle.id)
            logs = [item for item in records if item["kind"] == "node.log"]
            assert len(logs) == 2
            assert all(key in item["attributes"]["message"] for item in logs)
            assert all(item["attributes"]["step"] == 1 for item in logs)
            inputs = next(item for item in records if item["kind"] == "node.input")
            assert inputs["attributes"]["inputs"]["default"] == {
                "id": key,
                "password": "[REDACTED]",
            }
        before = len(service.store.events(handles[0][1].id))
        _LOGGER.warning("不属于任何节点的日志")
        assert len(service.store.events(handles[0][1].id)) == before
        service.close()


def test_generator_failure_preserves_output_logs_and_traceback(tmp_path: Path) -> None:
    """生成器的已产出项目和异常堆栈在服务重启后仍可读取。

    Args:
        tmp_path: 独立的诊断数据库目录。
    """

    class Stream(Node):
        """在输出一项后产生业务异常。"""

        def execute(self, inputs: Any, context: Any) -> Iterator[Output]:
            """产生输出和日志，然后失败。

            Args:
                inputs: 节点输入。
                context: 当前上下文。

            Yields:
                失败前已经产生的值。

            Raises:
                ValueError: 测试业务异常。
            """

            del inputs, context
            yield Output("first")
            _LOGGER.error("处理第二项失败")
            raise ValueError("second item failed")

    database = tmp_path / "details.sqlite3"
    with Runtime() as runtime:
        runtime.register("stream", Graph(entrypoint="stream").add(stream=Stream()))
        service = GraphService(runtime, database=database)
        handle = runtime.start("stream", None)
        with pytest.raises(Exception, match="second item failed"):
            handle.result()
        runtime.wait_idle()
        service.close()
    with Runtime() as runtime:
        service = GraphService(runtime, database=database)
        records = service.store.events(handle.id)
        assert (
            next(item for item in records if item["kind"] == "node.output")[
                "attributes"
            ]["output"]["value"]
            == "first"
        )
        failure = next(item for item in records if item["kind"] == "node.exception")
        assert "ValueError: second item failed" in failure["attributes"]["traceback"]
        finished = next(item for item in records if item["kind"] == "node.finished")
        assert "second item failed" in finished["attributes"]["traceback"]
        assert any(item["kind"] == "node.log" for item in records)
        service.close()


def test_diagnostics_can_be_disabled_and_close_preserves_logging_configuration() -> (
    None
):
    """关闭采集保留生命周期观测，卸载服务不改变根日志配置。"""

    root = logging.getLogger()
    handlers, level = tuple(root.handlers), root.level
    with Runtime() as runtime:
        runtime.register("echo", Graph(entrypoint="echo").add(echo=Echo()))
        service = GraphService(runtime, diagnostics=False)
        handle = runtime.start("echo", {"id": "disabled"})
        handle.result()
        runtime.wait_idle()
        assert all(
            item["kind"] not in {"node.input", "node.output", "node.log"}
            for item in service.store.events(handle.id)
        )
        service.close()
        enabled = GraphService(runtime)
        assert len(root.handlers) == len(handlers) + 1
        enabled.close()
        assert tuple(root.handlers) == handlers
        assert root.level == level
        assert (
            runtime.run("echo", {"id": "after-close"})[0].value["id"] == "after-close"
        )


def test_inspector_cannot_short_circuit_business_execution() -> None:
    """诊断方法抛出的控制信号不能接管节点执行。"""

    class Broken(NodeHook):
        """故意违反诊断权限的 Hook。"""

        def inspect(self, call: NodeCall, phase: HookPhase, value: Any) -> None:
            """尝试用控制信号干预执行。

            Args:
                call: 当前调用。
                phase: 当前采集阶段。
                value: 当前诊断值。

            Raises:
                StopGraph: 无权由诊断回调发出的控制信号。
            """

            del call, phase, value
            raise StopGraph(Output({"id": "wrong"}))

    with Runtime() as runtime:
        runtime.register("echo", Graph(entrypoint="echo").add(echo=Echo()))
        runtime.attach(Broken())
        assert runtime.run("echo", {"id": "correct"})[0].value == {"id": "correct"}


def test_diagnostics_do_not_consume_unserializable_domain_resources() -> None:
    """不支持编码的对象显示缺失标记，仍按原对象执行和输出。"""

    class Resource:
        """没有 JSON 编码契约的领域资源。"""

    class Pass(Node):
        """传递任意领域对象。"""

        def execute(self, inputs: Any, context: Any) -> Output:
            """返回原始输入对象。

            Args:
                inputs: 任意领域对象。
                context: 当前执行上下文。

            Returns:
                原对象的输出。
            """

            del context
            return Output(inputs["default"])

    resource = Resource()
    with Runtime() as runtime:
        runtime.register("resource", Graph(entrypoint="pass").add("pass", Pass()))
        service = GraphService(runtime)
        handle = runtime.start("resource", resource)
        assert handle.result()[0].value is resource
        runtime.wait_idle()
        records = service.store.events(handle.id)
        assert (
            "$unavailable"
            in next(item for item in records if item["kind"] == "node.input")[
                "attributes"
            ]["inputs"]["default"]
        )
        service.close()
