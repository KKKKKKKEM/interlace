"""服务传输模型与可视化图文档，不扩展核心 Graph 语义。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Model(BaseModel):
    """拒绝未知字段的服务文档基类。

    Attributes:
        model_config: 严格禁止拼错或未声明的字段。
    """

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Position(Model):
    """画布位置，仅影响编辑器。

    Attributes:
        x: 横坐标，单位为画布像素。
        y: 纵坐标，单位为画布像素。
    """

    x: float = 0
    y: float = 0


class NodeDefinition(Model):
    """通过登记的工厂创建一个节点绑定。

    Attributes:
        id: 图内唯一绑定标识。
        type: 节点目录中的具名工厂标识。
        config: 传给工厂配置模型的 JSON 数据。
        position: 节点画布位置，None 由编辑器自动布局。
    """

    id: str = Field(min_length=1, max_length=128)
    type: str = Field(min_length=1, max_length=256)
    config: dict[str, Any] = Field(default_factory=dict)
    position: Position | None = None


class EdgeDefinition(Model):
    """端口之间的有向连接。

    Attributes:
        source: 源节点绑定标识。
        target: 目标节点绑定标识。
        source_port: 源输出端口。
        target_port: 目标输入端口。
    """

    source: str
    target: str
    source_port: str = "default"
    target_port: str = "default"


class GraphDefinition(Model):
    """可保存草稿并编译为 Graph 的版本化文档。

    Attributes:
        name: 面向用户的图名称，不包含运行版本。
        entrypoint: 入口节点绑定标识，草稿允许尚未设置。
        nodes: 有序节点绑定集合。
        edges: 有序端口连线集合。
    """

    name: str = Field(min_length=1, max_length=128, pattern=r"^[\w.-]+$")
    entrypoint: str = ""
    nodes: list[NodeDefinition] = Field(default_factory=list, max_length=1000)
    edges: list[EdgeDefinition] = Field(default_factory=list, max_length=10000)


class DraftRequest(Model):
    """带乐观锁的草稿保存或发布请求。

    Attributes:
        definition: 完整图文档。
        revision: 预期草稿修订号，首次创建为零。
        project_id: 所属项目标识，省略时进入默认项目。
    """

    definition: GraphDefinition
    revision: int = Field(default=0, ge=0)
    project_id: str = Field(default="default", min_length=1)


class ProjectRequest(Model):
    """项目的可编辑管理信息。

    Attributes:
        name: 项目显示名称。
        description: 项目用途描述。
    """

    name: str = Field(min_length=1, max_length=128)
    description: str = Field(default="", max_length=2000)


class DefinitionProjectRequest(Model):
    """调整整条定义及其全部版本的管理归属。

    Attributes:
        name: 定义的全局标识。
        project_id: 目标项目标识。
    """

    name: str = Field(min_length=1)
    project_id: str = Field(min_length=1)


class RunRequest(Model):
    """所有调用协议共用的执行参数。

    Attributes:
        graph: 当前 Runtime 中的注册名称，发布图包含版本后缀。
        inputs: 按入口端口组织的 JSON 数据。
        options: 本次执行独立使用的领域配置。
        max_steps: 节点触发上限，零表示不限。
        timeout: Graph 执行时限，单位秒，None 表示不限。
    """

    graph: str = Field(min_length=1)
    inputs: dict[str, Any] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=dict)
    max_steps: int = Field(default=0, ge=0, strict=True)
    timeout: float | None = Field(default=None, gt=0, strict=True)
