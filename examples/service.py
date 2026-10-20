"""启动自带 HTTP 与 JSON-RPC 的图服务：uv run --extra service python examples/service.py。"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from interlace import Context, Graph, Node, Output, Runtime
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
        inputs: 接收文本的入口。
        outputs: 转换后文本的出口。
        config: 构造时复制的节点配置，不保存执行状态。
    """

    inputs = {"text": str}
    outputs = {"text": str}

    def __init__(self, config: TextConfig) -> None:
        """保存独立的构造配置。

        Args:
            config: 由目录校验后的文本节点配置。
        """

        self.config = config.model_copy(deep=True)

    async def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """等待后添加前缀、转换为大写，并追加本次执行配置的可选后缀。

        Args:
            inputs: text 端口的输入文本。
            context: 本次节点执行上下文；options["text"]["suffix"] 为可选后缀。

        Returns:
            text 端口上的转换结果。
        """

        _LOGGER.info("开始处理文本：%s", inputs["text"])
        await asyncio.sleep(self.config.delay)
        context.checkpoint()
        _LOGGER.info("文本处理完成")
        suffix = context.options.get("text", {}).get("suffix", "")
        return Output(self.config.prefix + inputs["text"].upper() + suffix, "text")


class SplitNode(Node):
    """将文本按行拆分为多个输出。

    Attributes:
        inputs: 接收多行文本。
        outputs: 逐行输出文本。
    """

    inputs = {"text": str}
    outputs = {"text": str}

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

    parser = argparse.ArgumentParser(description="Interlace graph service")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--database", default=".interlace/service.sqlite3")
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
        # 远程请求只能携带 text 命名空间的执行配置，其他 options 键返回 422。
        with GraphService(
            runtime,
            nodes=catalog,
            database=args.database,
            allowed_options=["text"],
        ) as service:
            service.serve(port=args.port)


if __name__ == "__main__":
    main()
