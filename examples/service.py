"""启动自带 RPC 与可视化工作台：uv run --extra service python examples/service.py。"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from interlace import Context, Graph, Node, Output, Ports, Runtime
from interlace.service import GraphService, NodeCatalog

# 示例主动启用 INFO；服务采集遵守应用已有的日志级别配置。
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)


class TextConfig(BaseModel):
    """文本处理节点的可编辑配置。

    Attributes:
        model_config: 禁止未知配置字段。
        prefix: 添加到文本开头的前缀。
        delay: 处理等待时间，单位秒。
    """

    model_config = ConfigDict(extra="forbid")
    prefix: str = Field(default="", title="文本前缀")
    delay: float = Field(default=0.4, ge=0, le=30, title="等待时间（秒）")


class TextNode(Node):
    """可重入的异步文本转换节点。

    Attributes:
        input_ports: 接收文本的入口。
        output_ports: 转换后文本的出口。
        config: 构造时复制的节点配置，不保存执行状态。
    """

    input_ports = Ports(text=str)
    output_ports = Ports(text=str)

    def __init__(self, config: TextConfig) -> None:
        """保存独立的构造配置。

        Args:
            config: 由目录校验后的文本节点配置。
        """

        self.config = config.model_copy(deep=True)

    async def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """等待后添加前缀并转换为大写。

        Args:
            inputs: text 端口的输入文本。
            context: 本次节点执行上下文。

        Returns:
            text 端口上的转换结果。
        """

        _LOGGER.info("开始处理文本：%s", inputs["text"])
        await asyncio.sleep(self.config.delay)
        context.checkpoint()
        _LOGGER.info("文本处理完成")
        return Output(self.config.prefix + inputs["text"].upper(), "text")


class SplitNode(Node):
    """将文本按行拆分为多个输出。

    Attributes:
        input_ports: 接收多行文本。
        output_ports: 逐行输出文本。
    """

    input_ports = Ports(text=str)
    output_ports = Ports(text=str)

    def execute(self, inputs: Mapping[str, Any], context: Context) -> list[Output]:
        """按行产生独立数据流 token。

        Args:
            inputs: 多行文本。
            context: 本次执行上下文。

        Returns:
            每行对应的 text 输出。
        """

        del context
        return [Output(line, "text") for line in inputs["text"].splitlines()]


def main() -> None:
    """创建示例节点目录并启动可选服务。"""

    parser = argparse.ArgumentParser(description="Interlace Studio")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--database", default=".interlace/studio.sqlite3")
    args = parser.parse_args()
    catalog = NodeCatalog().register(
        "text.transform", TextNode, config=TextConfig, title="文本转换"
    )
    catalog.register_node("text.split", SplitNode, title="按行拆分")
    graph = Graph(entrypoint="split").add(
        split=SplitNode(),
        transform=TextNode(TextConfig()),
        finish=TextNode(TextConfig(prefix="READY: ", delay=0.15)),
    )
    graph.connect("split", "transform", source_port="text", target_port="text")
    graph.connect("transform", "finish", source_port="text", target_port="text")
    with Runtime() as runtime:
        runtime.register("text.pipeline", graph)
        service = GraphService(runtime, nodes=catalog, database=args.database)
        try:
            service.serve(port=args.port)
        finally:
            service.close()


if __name__ == "__main__":
    main()
