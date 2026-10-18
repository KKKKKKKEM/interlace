"""事件路由：一张 Graph 发布事件，另一张 Graph 在队列中消费它。"""

from __future__ import annotations

from interlace import Graph, Node, Ports, Runtime


class Publish(Node):
    """通过上下文发布跨图事件的示例节点。

    Attributes:
        input_ports: 输入端口，message 为要发布的消息文本。
        output_ports: 空输出端口；节点只发布事件，不产生 Output。
    """

    input_ports = Ports(message=str)
    output_ports = Ports()

    def execute(self, inputs, context):
        """把 message 作为 message.created 事件发布，不产生本图 Output。

        Args:
            inputs: 按输入端口名索引的值，message 为事件载荷。
            context: Node 执行上下文，用于调用 emit 发布事件。
        """

        context.emit("message.created", inputs["message"])


class Collect(Node):
    """将事件输入追加到调用方消息列表的示例节点。

    Attributes:
        input_ports: 输入端口，message 接收路由过来的事件载荷。
        output_ports: 空输出端口；节点只记录消息，不产生 Output。
        _received: 调用方传入的消息列表，节点向其追加收到的消息以便观察结果。
    """

    input_ports = Ports(message=str)
    output_ports = Ports()

    def __init__(self, received: list[str]) -> None:
        """保存调用方提供的消息收集列表。

        Args:
            received: 调用方持有的列表；节点只追加，不清空或替换。
        """

        self._received = received

    def execute(self, inputs, context):
        """把 message 追加到收集列表，不产生 Output。

        Args:
            inputs: 按输入端口名索引的值，message 为事件载荷。
            context: Node 执行上下文；本示例不使用。
        """

        del context
        self._received.append(inputs["message"])


def run(message: str = "hello") -> list[str]:
    """发布事件，等待路由后的 Graph 完成。

    Args:
        message: 需要从生产 Graph 路由到消费 Graph 的消息文本。

    Returns:
        消费 Graph 接收到的消息列表。
    """

    received: list[str] = []
    producer = Graph(entrypoint="publish").add(publish=Publish())
    consumer = Graph(entrypoint="collect").add(collect=Collect(received))
    with Runtime() as runtime:
        runtime.register("producer", producer)
        runtime.register("consumer", consumer)
        runtime.on(
            "message.created",
            graph="consumer",
            queue="messages",
            concurrency=2,
        )
        runtime.run("producer", message)
        runtime.wait_idle()
    return received


if __name__ == "__main__":
    print(run())
