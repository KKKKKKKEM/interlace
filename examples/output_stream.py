"""流式消费多个 terminal Output，并在结束后读取完整结果。"""

from __future__ import annotations

import asyncio

from interlace import Graph, Node, Output, Runtime


class ProduceMany(Node):
    """逐项产生终端输出以展示流式交付的示例节点。

    Attributes:
        inputs: 输入端口，count 为要产生的输出数量。
        outputs: 输出端口，item 未连线，每项都作为终端输出交付。
    """

    inputs = {"count": int}
    outputs = {"item": int}

    def execute(self, inputs, context):
        """一次返回 count 项 item 输出，供调用方逐项流式消费。

        Args:
            inputs: 按输入端口名索引的值，count 为输出数量。
            context: Node 执行上下文；本示例不使用。

        Returns:
            item 端口上从 0 到 count - 1 的整数 Output 元组。
        """

        del context
        return tuple(Output(value, "item") for value in range(inputs["count"]))


def make_graph() -> Graph:
    """构造只含 ProduceMany 一个节点的示例 Graph。

    Returns:
        以 produce 为入口、尚未注册的 Graph。
    """

    return Graph(entrypoint="produce").add(produce=ProduceMany())


def run(count: int = 4) -> tuple[list[int], list[int]]:
    """同步迭代 Execution，并保留最终结果快照。

    Args:
        count: 示例需要产生的输出数量。

    Returns:
        流式消费得到的整数列表，以及同一执行的完整结果列表。
    """

    with Runtime() as runtime:
        runtime.register("output-stream", make_graph())
        execution = runtime.start("output-stream", count, output_buffer=2)

        streamed = [output.value for output in execution]
        completed = [output.value for output in execution.result()]
        return streamed, completed


async def arun(count: int = 4) -> tuple[list[int], list[int]]:
    """异步迭代便利接口，并直接 await 另一项 Execution。

    Args:
        count: 示例需要产生的输出数量。

    Returns:
        异步流式消费得到的整数列表，以及另一项执行的完整结果列表。
    """

    with Runtime() as runtime:
        runtime.register("output-stream", make_graph())
        streamed = [
            output.value
            async for output in runtime.aiter(
                "output-stream",
                count,
                output_buffer=2,
            )
        ]

        completed = await runtime.start("output-stream", count)
        return streamed, [output.value for output in completed]


if __name__ == "__main__":
    sync_stream, sync_result = run()
    async_stream, async_result = asyncio.run(arun())
    print("sync stream:", sync_stream)
    print("sync result:", sync_result)
    print("async stream:", async_stream)
    print("async result:", async_result)
