"""异步 Node：Graph 语义不变，Node 自己可以 await I/O。"""

from __future__ import annotations

import asyncio

from interlace import Graph, Node, Output, Runtime


class DelayedUpper(Node):
    """异步等待后将输入文本转为大写的示例节点。

    Attributes:
        inputs: 输入端口，text 为待转换的字符串。
        outputs: 输出端口，result 为转换后的大写字符串。
    """

    inputs = {"text": str}
    outputs = {"result": str}

    async def execute(self, inputs, context):
        """模拟一次异步 I/O 等待，再把 text 转为大写。

        Args:
            inputs: 按输入端口名索引的值，text 为待转换文本。
            context: Node 执行上下文；本示例不使用。

        Returns:
            result 端口上的大写文本 Output。
        """

        del context
        await asyncio.sleep(0.01)
        return Output(inputs["text"].upper(), "result")


def run(text: str = "asynchronous") -> str:
    """同步执行 Graph 并返回终端输出。

    Args:
        text: 示例需要转换的输入文本。

    Returns:
        result 端口上的大写文本。
    """

    graph = Graph(entrypoint="upper").add(upper=DelayedUpper())
    with Runtime() as runtime:
        runtime.register("async-node", graph)
        return runtime.run("async-node", text)[0].value


if __name__ == "__main__":
    print(run())
