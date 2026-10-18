"""线性 Graph：清理文本，然后转换为大写。"""

from __future__ import annotations

from interlace import Graph, Node, Output, Runtime


class Strip(Node):
    """移除输入文本首尾空白的示例节点。

    Attributes:
        inputs: 输入端口，text 为原始文本。
        outputs: 输出端口，text 为去除首尾空白后的文本。
    """

    inputs = {"text": str}
    outputs = {"text": str}

    def execute(self, inputs, context):
        """去除 text 首尾空白后传给下游。

        Args:
            inputs: 按输入端口名索引的值，text 为原始文本。
            context: Node 执行上下文；本示例不使用。

        Returns:
            text 端口上已去除首尾空白的文本 Output。
        """

        del context
        return Output(inputs["text"].strip(), "text")


class Upper(Node):
    """将输入文本转换为大写的示例节点。

    Attributes:
        inputs: 输入端口，text 为待转换文本。
        outputs: 输出端口，result 为大写文本；该端口未连线，成为终端输出。
    """

    inputs = {"text": str}
    outputs = {"result": str}

    def execute(self, inputs, context):
        """把 text 转为大写后从 result 输出。

        Args:
            inputs: 按输入端口名索引的值，text 为待转换文本。
            context: Node 执行上下文；本示例不使用。

        Returns:
            result 端口上的大写文本 Output。
        """

        del context
        return Output(inputs["text"].upper(), "result")


def run(text: str = "  hello, interlace  ") -> str:
    """执行一条两节点的数据流，并返回未连接的终端输出。

    Args:
        text: 原始输入文本，可包含首尾空白。

    Returns:
        去除首尾空白并转为大写后的文本。
    """

    graph = (
        Graph(entrypoint="strip")
        .add(strip=Strip(), upper=Upper())
        .connect("strip", "upper", source_port="text", target_port="text")
    )
    with Runtime() as runtime:
        runtime.register("linear", graph)
        return runtime.run("linear", text)[0].value


if __name__ == "__main__":
    print(run())
