"""执行配置的公开入口、快照和隔离契约。"""

import asyncio
from threading import Event as ThreadEvent

import pytest

from interlace import Context, Graph, Node, Output, Ports, Runtime
from interlace.spi import Work
from interlace.adapters import memory
from interlace.runtime import GraphWorker


class ReadOptions(Node):
    """返回当前执行的配置供调用方校验。"""

    input_ports = Ports(default=int)  # 用于触发当前调用的整数。
    output_ports = Ports(default=dict)  # 当前调用配置的字典快照。

    def execute(self, inputs, context):
        """输出当前配置。

        Args:
            inputs: 当前调用的整数输入。
            context: 含本次执行配置的上下文。

        Returns:
            当前执行配置。
        """

        return Output(dict(context.options))


@pytest.mark.parametrize("method", ["run", "start", "iter", "arun", "aiter"])
def test_runtime_options_on_all_entrypoints(method):
    """验证同步、异步和流式入口共享配置契约。

    Args:
        method: 本次检查的公开入口名称。
    """

    async def collect(runtime):
        """收集异步入口的终端输出。

        Args:
            runtime: 已注册测试 Graph 的运行时。

        Returns:
            本次异步执行的完整输出。
        """

        if method == "arun":
            return await runtime.arun("read", 1, options={"parser.strict": True})
        return tuple(
            [
                item
                async for item in runtime.aiter(
                    "read", 1, options={"parser.strict": True}
                )
            ]
        )

    with Runtime() as runtime:
        runtime.register("read", Graph(entrypoint="read").add(read=ReadOptions()))
        if method in {"arun", "aiter"}:
            outputs = asyncio.run(collect(runtime))
        else:
            result = getattr(runtime, method)(
                "read", 1, options={"parser.strict": True}
            )
            outputs = result.result() if method == "start" else tuple(result)
        assert outputs == (Output({"parser.strict": True}),)
        assert runtime.run("read", 1) == (Output({}),)


def test_context_and_work_snapshot_options():
    """验证配置深复制、顶层只读和嵌套值隔离。"""

    original = {"parser.rules": ["strict"]}
    first = Context(lambda event: None, options=original)
    second = Context(lambda event: None, options=original)
    work = Work("read", options=original)
    original["parser.rules"].append("changed")
    first.options["parser.rules"].append("local")
    assert second.options == {"parser.rules": ["strict"]}
    assert work.options == {"parser.rules": ["strict"]}
    with pytest.raises(TypeError):
        first.options["parser.strict"] = True


def test_task_adapter_delivers_work_options():
    """验证公开 Work 配置可以通过任务适配器传到节点。"""

    tasks = memory.TaskBackend()
    try:
        with GraphWorker(consumer=tasks) as worker:
            worker.register("read", Graph(entrypoint="read").add(read=ReadOptions()))
            worker.consume("read")
            work = Work("read", 1, options={"parser.strict": True})
            tasks.submit("read", work)
            tasks.wait_idle(timeout=5)
            assert worker.get_execution(work.id).result() == (
                Output({"parser.strict": True}),
            )
    finally:
        tasks.close()


def test_start_snapshots_before_background_execution():
    """验证提交后修改调用方配置不会影响后台任务。"""

    entered = ThreadEvent()
    release = ThreadEvent()

    class Wait(Node):
        """阻塞首节点以检查后续节点的配置快照。"""

        input_ports = Ports(default=int)  # 原样传递的触发值。
        output_ports = Ports(default=int)  # 后续节点的触发值。

        def execute(self, inputs, context):
            """等待外部修改原始配置后再输出。

            Args:
                inputs: 整数输入。
                context: 当前执行上下文。

            Returns:
                原输入值。
            """

            entered.set()
            assert release.wait(5)
            context.options["parser.rules"].append("local")
            return Output(inputs["default"])

    graph = (
        Graph(entrypoint="wait")
        .add(wait=Wait(), read=ReadOptions())
        .connect("wait", "read")
    )
    options = {"parser.rules": ["strict"]}
    with Runtime() as runtime:
        runtime.register("read", graph)
        execution = runtime.start("read", 1, options=options)
        try:
            assert entered.wait(5)
            options["parser.rules"].append("changed")
        finally:
            release.set()
        assert execution.result() == (Output({"parser.rules": ["strict"]}),)


def test_event_execution_does_not_inherit_options():
    """验证跨图事件不会隐式继承源执行配置。"""

    class Emit(Node):
        """发出独立的目标图事件。"""

        input_ports = Ports(default=int)  # 发出的事件数据。

        def execute(self, inputs, context):
            """显式发布目标事件。

            Args:
                inputs: 原始整数输入。
                context: 含源执行配置的上下文。
            """

            context.emit("read", inputs["default"])

    with Runtime() as runtime:
        runtime.register("source", Graph(entrypoint="emit").add(emit=Emit()))
        runtime.register("target", Graph(entrypoint="read").add(read=ReadOptions()))
        runtime.on("read", graph="target", queue="target")
        runtime.run("source", 1, options={"parser.strict": True})
        runtime.wait_idle(timeout=5)
        target = next(item for item in runtime.executions() if item.graph == "target")
        assert target.result() == (Output({}),)


@pytest.mark.parametrize("options", [[], {"": True}, {1: True}])
def test_invalid_options_fail_before_submission(options):
    """验证非法配置不会产生待执行任务。

    Args:
        options: 非映射或含非法键的配置。
    """

    with Runtime() as runtime:
        runtime.register("read", Graph(entrypoint="read").add(read=ReadOptions()))
        with pytest.raises((TypeError, ValueError)):
            runtime.start("read", 1, options=options)
        assert runtime.executions() == ()
