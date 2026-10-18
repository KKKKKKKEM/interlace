"""协议与工作台共用的官方图服务。"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from queue import SimpleQueue
from threading import Lock, RLock, Thread
from types import TracebackType
from typing import Any

from pydantic import TypeAdapter
from pydantic_core import to_jsonable_python

from interlace import Graph, Runtime
from interlace.engine import Execution
from interlace.engine.errors import InterlaceRuntimeError
from interlace.plugins import Registration
from interlace.plugins.points import (
    RUNTIME_EVENT,
    RuntimeEvent,
    RuntimeEventKind,
)

from .catalog import NodeCatalog, describe_graph
from .diagnostics import (
    NodeDiagnostics,
    contains_iterator,
    redact_text,
    redact_value,
    snapshot_value,
)
from .errors import (
    ConflictError,
    InvalidRequestError,
    NotFoundError,
    OutputEncodingError,
    ServiceCapacityError,
    ServiceClosedError,
)
from .models import DraftRequest, GraphDefinition, NodeDefinition, RunRequest
from .store import ServiceStore, registered_name

_LOGGER = logging.getLogger(__name__)
# 默认 Host 白名单中的本机名称；serve() 另外加入实际监听地址。
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def encode(value: Any) -> Any:
    """将支持的 Python 值编码为严格 JSON；不使用 repr 隐式降级。

    Args:
        value: 内置值、数据类或具有 Pydantic 序列化规则的对象。

    Returns:
        可无损表示于 JSON 的结构。

    Raises:
        OutputEncodingError: 对象无法通过声明的序列化规则编码，或包含会被序列化耗尽的
            一次性迭代器。
    """

    if contains_iterator(value):
        raise OutputEncodingError(
            f"output contains a one-shot iterator: {type(value).__name__}"
        )
    try:
        result = to_jsonable_python(value, serialize_unknown=False)
        json.dumps(result, allow_nan=False)
        return result
    except (TypeError, ValueError) as exc:
        raise OutputEncodingError(
            f"output is not JSON serializable: {type(value).__name__}"
        ) from exc


@dataclass(frozen=True, slots=True)
class _Publication:
    """一个已注册发布版本的内存索引项，避免观测热路径解码数据库。

    Attributes:
        name: 逻辑定义标识。
        version: 从 1 开始的版本号。
        definition: 发布时保存的图文档，只读使用。
        node_configs: 按节点绑定标识索引的脱敏构造配置快照。
    """

    name: str
    version: int
    definition: dict[str, Any]
    node_configs: dict[str, Any]

    @classmethod
    def of(cls, name: str, version: int, definition: dict[str, Any]) -> _Publication:
        """从保存的文档建立索引项并预先生成节点配置快照。"""

        return cls(
            name,
            version,
            definition,
            {
                item["id"]: snapshot_value(item["config"])
                for item in definition["nodes"]
            },
        )

    def view(self) -> dict[str, Any]:
        """返回接口展示用的发布信息。"""

        return {
            "name": self.name,
            "version": self.version,
            "definition": self.definition,
        }


class GraphService:
    """按需启用 RPC 和 Studio，共用调用方的 Runtime。

    Attributes:
        runtime: 调用方管理生命周期的运行时。
        nodes: 显式登记的编辑器节点目录。
        store: 当前服务独占的文档和观测数据库。
        max_active_executions: 通过服务提交时允许 Runtime 同时存在的活跃执行数量。
        allowed_options: 远程请求允许携带的 options 顶层键；空集合表示拒绝非空 options。
        _closed: 服务是否已关闭。
        _start_lock: 串行执行容量检查与提交，避免并发请求越过上限。
        _publications: 注册名到发布版本索引项，在启动恢复和发布时维护。
        _descriptions: 注册名到图结构描述的缓存；注册名在 Runtime 生命周期内不变。
        _diagnostics: 节点参数与日志采集器，关闭采集时为 None。
        _observer: 服务持有的 RUNTIME_EVENT 登记，安装前为 None。
        _adapters: 端口类型到 TypeAdapter 的缓存，避免每次请求重建校验器。
        _archive_queue: 等待终态后归档输出的执行，None 通知归档线程退出。
        _archiver: 在 Worker 线程之外等待执行终止并归档输出的线程。
        _attempts: 执行 ID 到最近一次开始的 Execution；同 ID 重投递时据此跳过被取代的旧尝试，
            归档完成后移除。
        _attempts_lock: 串行化新尝试的重置与旧尝试的归档写入。
    """

    def __init__(
        self,
        runtime: Runtime,
        *,
        nodes: NodeCatalog | None = None,
        database: str | Path = ":memory:",
        diagnostics: bool = True,
        max_active_executions: int = 128,
        allowed_options: Iterable[str] | None = None,
        retention: int | None = 1000,
    ) -> None:
        """接入已有 Runtime，并恢复已发布的图定义。

        恢复的发布版本最后注册：之前任何步骤失败都会卸载观察、诊断并关闭数据库，
        Runtime 不留下服务注册，可以直接重建。Runtime 不提供注销接口，注册阶段本身失败
        （例如 Runtime 并发关闭）时已注册的版本保留在该 Runtime 中。

        Args:
            runtime: 已有运行时，必须提供公开 GraphCatalog 能力。
            nodes: 编辑器可构造的节点类型目录，None 使用空目录。
            database: 服务数据库路径，默认仅内存保存。
            diagnostics: 是否采集实际节点输入、原始输出和 Python 日志，默认启用。
            max_active_executions: 服务允许的活跃执行上限，默认 128。
            allowed_options: 远程 RunRequest.options 允许的顶层键，None 拒绝任何非空 options。
            retention: 数据库最多保留的执行记录数，默认 1000，None 表示不清理。

        Raises:
            ValueError: max_active_executions 或 retention 不是正整数。
            TypeError: allowed_options 是字符串，或 Worker 不支持公开图目录。
            ConflictError: 已保存版本的注册名已被 Runtime 占用。
            InvalidRequestError: 已保存版本引用了未登记的节点类型。
            pydantic.ValidationError: 已保存版本的节点配置不再符合配置模型。
            GraphError: 已保存版本无法通过冻结校验。
            RuntimeError: 数据库结构版本高于当前代码支持的版本。
        """

        if type(max_active_executions) is not int or max_active_executions < 1:
            raise ValueError(
                "max_active_executions must be an integer greater than zero"
            )
        if isinstance(allowed_options, str):
            raise TypeError("allowed_options must be an iterable of option keys")
        registered = runtime.graphs()
        self.runtime = runtime
        self.nodes = nodes or NodeCatalog()
        self.max_active_executions = max_active_executions
        self.allowed_options = frozenset(allowed_options or ())
        self._closed = False
        self._start_lock = RLock()
        self._publications: dict[str, _Publication] = {}
        self._descriptions: dict[str, dict[str, Any]] = {}
        self._adapters: dict[Any, TypeAdapter[Any]] = {}
        self._diagnostics: NodeDiagnostics | None = None
        self._observer: Registration | None = None
        self._archive_queue: SimpleQueue[Execution | None] = SimpleQueue()
        self._archiver = Thread(
            target=self._archive_loop, name="interlace-service-archive", daemon=True
        )
        self._attempts: dict[str, Execution] = {}
        self._attempts_lock = Lock()
        self.store = ServiceStore(database, retention=retention)
        try:
            restored = []
            for item in self.store.versions():
                definition = GraphDefinition.model_validate(item["definition"])
                restored.append(
                    (
                        registered_name(item["name"], item["version"]),
                        _Publication.of(
                            item["name"], item["version"], item["definition"]
                        ),
                        self._compile(definition),
                    )
                )
            if any(name in registered for name, _, _ in restored):
                raise ConflictError(
                    "saved graph version conflicts with Runtime registration"
                )
            self.store.interrupt_unfinished("服务已重启，运行未恢复")
            self.store.adopt_definitions(registered)
            self._archiver.start()
            if diagnostics:
                self._diagnostics = NodeDiagnostics(runtime, self.store)
            self._observer = runtime.hooks.on(RUNTIME_EVENT, self._observe)
            for name, publication, graph in restored:
                self._publications[name] = publication
                runtime.register(name, graph)
        except BaseException:
            self._release()
            raise

    def graphs(self) -> list[dict[str, Any]]:
        """读取 Runtime 中全部注册图的结构，发布版本附带保存的文档。

        Returns:
            每个注册名一项，包含图结构和 publication（代码图为 None）。
        """

        return [
            {**self._describe(name), "publication": self._publication_view(name)}
            for name in self.runtime.graphs()
        ]

    def validate(self, definition: GraphDefinition) -> dict[str, Any]:
        """构造并冻结草稿以执行核心校验，不发布版本。

        Args:
            definition: 待校验文档。

        Returns:
            冻结后的真实端口与图结构。

        Raises:
            InvalidRequestError: 节点类型未登记。
            pydantic.ValidationError: 节点配置非法。
            GraphError: 图未通过冻结校验。
        """

        return describe_graph(definition.name, self._compile(definition))

    def node_types(self) -> list[dict[str, Any]]:
        """读取编辑器可用的节点类型及配置表单 Schema。

        Returns:
            节点类型、显示名称及 JSON Schema 列表。
        """

        return self.nodes.describe()

    def preview_node(self, node: NodeDefinition) -> dict[str, Any]:
        """通过登记的工厂构造单个节点并读取其真实端口。

        Args:
            node: 节点类型与配置。

        Returns:
            inputs 与 outputs 两项，各自为端口名到类型名的映射。

        Raises:
            InvalidRequestError: 节点类型未登记。
            pydantic.ValidationError: 节点配置非法。
        """

        created = self.nodes.create(node.type, node.config)
        return {
            "inputs": {key: value.__name__ for key, value in created.inputs.items()},
            "outputs": {key: value.__name__ for key, value in created.outputs.items()},
        }

    def projects(self) -> list[dict[str, Any]]:
        """读取项目及其定义数量与草稿数量。

        Returns:
            项目元数据，附带 definition_count 与 draft_count。
        """

        definitions = self.definitions()
        return [
            {
                **project,
                "definition_count": sum(
                    item["project_id"] == project["id"] for item in definitions
                ),
                "draft_count": sum(
                    item["project_id"] == project["id"]
                    and item["draft_revision"] is not None
                    for item in definitions
                ),
            }
            for project in self.store.projects()
        ]

    def project(self, project_id: str) -> dict[str, Any]:
        """读取单个项目。

        Args:
            project_id: 项目稳定标识。

        Returns:
            项目元数据。

        Raises:
            NotFoundError: 项目不存在。
        """

        return self.store.project(project_id)

    def create_project(self, name: str, description: str) -> dict[str, Any]:
        """创建管理项目。

        Args:
            name: 项目显示名称，必须唯一且非空。
            description: 项目描述。

        Returns:
            新项目的元数据。

        Raises:
            InvalidRequestError: 名称为空。
            ConflictError: 名称已存在。
        """

        return self.store.save_project(name, description)

    def update_project(
        self, project_id: str, name: str, description: str
    ) -> dict[str, Any]:
        """修改项目名称与描述，保留项目标识。

        Args:
            project_id: 目标项目。
            name: 新显示名称。
            description: 新描述。

        Returns:
            更新后的项目元数据。

        Raises:
            NotFoundError: 项目不存在。
            InvalidRequestError: 名称为空。
            ConflictError: 名称已被其他项目使用。
        """

        return self.store.save_project(name, description, project_id)

    def delete_project(self, project_id: str) -> None:
        """删除不含定义的非默认项目。

        Args:
            project_id: 待删除项目。

        Raises:
            NotFoundError: 项目不存在。
            ConflictError: 默认项目或项目仍包含定义。
        """

        self.store.delete_project(project_id)

    def definitions(self, project_id: str | None = None) -> list[dict[str, Any]]:
        """按逻辑定义合并草稿、发布版本、代码注册与运行统计，只读不写数据库。

        尚未持久化归属的代码图按默认项目展示。

        Args:
            project_id: 限定项目，None 返回全部定义。

        Returns:
            按名称排序的定义摘要，每条逻辑定义一行。

        Raises:
            NotFoundError: project_id 指定的项目不存在。
        """

        if project_id is not None:
            self.store.project(project_id)
        graphs = self.runtime.graphs()
        latest: dict[str, tuple[str, _Publication]] = {}
        code: set[str] = set()
        for name in graphs:
            publication = self._publications.get(name)
            if publication is None:
                code.add(name)
            elif (
                publication.name not in latest
                or latest[publication.name][1].version < publication.version
            ):
                latest[publication.name] = (name, publication)
        memberships = self.store.memberships()
        for name in code:
            memberships.setdefault(name, {"project_id": "default", "updated_at": None})
        drafts = {item["definition"]["name"]: item for item in self.store.drafts()}
        summary = self._run_summary()
        rows = []
        for name, membership in memberships.items():
            if project_id is not None and membership["project_id"] != project_id:
                continue
            published = latest.get(name)
            draft = drafts.get(name)
            graph = published[0] if published else name if name in code else None
            count, last_run = summary.get(name, (0, None))
            if published:
                status = "published"
            elif name in code:
                status = "code"
            else:
                status = "draft" if draft else "unavailable"
            if graph is not None:
                node_count = len(graphs[graph].nodes)
            else:
                node_count = len(draft["definition"]["nodes"]) if draft else 0
            rows.append(
                {
                    "name": name,
                    **membership,
                    "source": "visual" if draft or published else "code",
                    "status": status,
                    "graph": graph,
                    "version": published[1].version if published else None,
                    "draft_revision": draft["revision"] if draft else None,
                    "node_count": node_count,
                    "last_run": last_run,
                    "run_count": count,
                }
            )
        return sorted(rows, key=lambda row: row["name"])

    def definition(self, name: str, project_id: str | None = None) -> dict[str, Any]:
        """取得单条定义的管理详情、版本、草稿与运行记录。

        Args:
            name: 定义的全局标识。
            project_id: 可选项目范围；定义不属于该项目时视为不存在。

        Returns:
            定义摘要附带 project、graphs（按版本倒序）、draft 与 runs。

        Raises:
            NotFoundError: 定义不存在或不属于指定项目。
        """

        summary = next(
            (item for item in self.definitions() if item["name"] == name), None
        )
        if summary is None or (
            project_id is not None and summary["project_id"] != project_id
        ):
            raise NotFoundError(f"定义不存在：{name}")
        graphs = [
            {**self._describe(item), "publication": self._publication_view(item)}
            for item in self.runtime.graphs()
            if self._definition_name(item) == name
        ]
        graphs.sort(
            key=lambda item: (
                item["publication"]["version"] if item["publication"] else 0
            ),
            reverse=True,
        )
        return {
            **summary,
            "project": self.store.project(summary["project_id"]),
            "graphs": graphs,
            "draft": self.store.draft(name),
            "runs": self.executions(definition_name=name),
        }

    def create_definition(
        self, project_id: str, definition: GraphDefinition
    ) -> dict[str, Any]:
        """在项目中创建新定义的首个草稿，不覆盖任何已有定义。

        Args:
            project_id: 所属项目。
            definition: 初始文档，允许尚未满足冻结约束。

        Returns:
            新定义的管理详情。

        Raises:
            NotFoundError: 项目不存在。
            ConflictError: 定义标识已被代码图、草稿、版本或历史记录占用。
        """

        self.save_draft(
            DraftRequest(definition=definition, revision=0, project_id=project_id)
        )
        return self.definition(definition.name)

    def drafts(self) -> list[dict[str, Any]]:
        """读取全部编辑草稿。

        Returns:
            按名称排列的草稿文档、修订号和所属项目。
        """

        return self.store.drafts()

    def save_draft(self, request: DraftRequest) -> int:
        """保存可不完整的草稿；新标识必须全局唯一，已有草稿比较修订号。

        Args:
            request: 草稿文档、预期修订号与所属项目。

        Returns:
            新修订号。

        Raises:
            NotFoundError: 项目不存在。
            ConflictError: 修订号已过期、标识属于其他项目或已被其他定义占用。
        """

        self._adopt_code_graphs()
        return self.store.save_draft(
            request.definition.name,
            request.definition.model_dump(),
            request.revision,
            request.project_id,
        )

    def move_definition(self, name: str, project_id: str) -> dict[str, Any]:
        """整体移动定义的项目归属，不改变版本、注册名或执行事实。

        Args:
            name: 定义全局标识。
            project_id: 目标项目。

        Returns:
            移动后的定义详情。

        Raises:
            NotFoundError: 定义或目标项目不存在。
        """

        self.definition(name)
        self.store.assign_definition(name, project_id)
        return self.definition(name)

    def publish(self, request: DraftRequest) -> dict[str, Any]:
        """冻结校验后发布新版本并注册为 `name@vN`，历史版本保持不变。

        版本、草稿与归属在同一事务中写入，Runtime 注册失败时整体回滚。

        Args:
            request: 基于当前草稿修订号的完整图文档。

        Returns:
            graph（注册名）、version 与更新后的 revision。

        Raises:
            InvalidRequestError: 节点类型未登记。
            pydantic.ValidationError: 节点配置非法。
            GraphError: 图未通过冻结校验。
            NotFoundError: 项目不存在。
            ConflictError: 修订号已过期、标识属于其他项目或已被其他定义占用。
        """

        graph = self._compile(request.definition)
        name = request.definition.name
        body = request.definition.model_dump()
        self._adopt_code_graphs()

        def register(version: int) -> None:
            """在数据库事务提交前向 Runtime 注册新版本并加入内存索引。"""

            registered = registered_name(name, version)
            self.runtime.register(registered, graph)
            self._publications[registered] = _Publication.of(name, version, body)

        version = self.store.publish(
            name, body, request.revision, request.project_id, register
        )
        return {
            "graph": registered_name(name, version),
            "version": version,
            "revision": request.revision + 1,
        }

    def start(self, request: RunRequest) -> Execution:
        """按端口类型解码 JSON 并通过 Runtime 提交执行。

        Args:
            request: 协议无关的执行参数，inputs 始终按端口命名。

        Returns:
            可等待、取消和迭代的原生执行句柄。

        Raises:
            ServiceClosedError: 服务已关闭。
            NotFoundError: 图未注册。
            InvalidRequestError: options 含未允许的键，或 inputs 端口与入口不一致。
            pydantic.ValidationError: 输入值不符合端口类型。
            ServiceCapacityError: 活跃执行达到 max_active_executions。
            RuntimeClosedError: Runtime 已开始关闭。
        """

        if self._closed:
            raise ServiceClosedError("graph service is closed")
        graph = self.runtime.graphs().get(request.graph)
        if graph is None:
            raise NotFoundError(f"unknown registered graph: {request.graph}")
        rejected = sorted(set(request.options) - self.allowed_options)
        if rejected:
            raise InvalidRequestError(f"options keys are not allowed: {rejected}")
        ports = graph.spec_for(graph.entrypoint).input_ports
        if set(request.inputs) != set(ports):
            raise InvalidRequestError(
                f"inputs must contain exactly these ports: {list(ports)}"
            )
        inputs = {
            port: self._adapter(typ).validate_json(
                json.dumps(request.inputs[port]), strict=True
            )
            for port, typ in ports.items()
        }
        with self._start_lock:
            active = sum(not item.done for item in self.runtime.executions())
            if active >= self.max_active_executions:
                raise ServiceCapacityError(
                    "graph service reached max_active_executions="
                    f"{self.max_active_executions}"
                )
            execution = self.runtime.start(
                request.graph,
                next(iter(inputs.values())) if len(ports) == 1 else inputs,
                options=request.options,
                max_steps=request.max_steps,
                timeout=request.timeout,
                max_queued=request.max_queued,
            )
        self.store.record_execution(self.snapshot(execution))
        if execution.done:
            self._archive(execution)
        return execution

    def snapshot(self, execution: Execution) -> dict[str, Any]:
        """读取执行状态，不物化输出；错误消息经过凭据脱敏。

        Args:
            execution: 当前进程的执行句柄。

        Returns:
            可用于列表和历史记录的状态快照。
        """

        publication = self._publications.get(execution.graph)
        error = execution.error
        return {
            "id": execution.id,
            "graph": execution.graph,
            "definition_name": self._definition_name(execution.graph),
            "node_configs": {} if publication is None else publication.node_configs,
            "diagnostics": self._diagnostics is not None,
            "graph_snapshot": self._describe(execution.graph),
            "status": execution.status.value,
            "done": execution.done,
            "steps": execution.steps,
            "current_node": execution.current_node,
            "started_at": execution.started_at.isoformat()
            if execution.started_at
            else None,
            "finished_at": execution.finished_at.isoformat()
            if execution.finished_at
            else None,
            "error": None if error is None else redact_text(str(error)),
            "error_type": None if error is None else type(error).__name__,
            "cancel_requested": execution.cancel_requested,
        }

    def live_execution(self, execution_id: str) -> Execution | None:
        """返回当前 Runtime 仍保留的执行句柄。

        Args:
            execution_id: 执行标识。

        Returns:
            执行句柄；已被 Runtime 淘汰或来自其他进程时为 None。
        """

        try:
            return self.runtime.get_execution(execution_id)
        except (InterlaceRuntimeError, ValueError):
            return None

    def execution(self, execution_id: str) -> dict[str, Any]:
        """优先读取当前句柄，已经离开 Runtime 的执行读取历史快照。

        Args:
            execution_id: 目标执行标识。

        Returns:
            执行状态快照。

        Raises:
            NotFoundError: 当前与历史都没有此执行。
        """

        live = self.live_execution(execution_id)
        if live is not None:
            return self.snapshot(live)
        return self.store.execution(execution_id)

    def executions(
        self, *, definition_name: str | None = None, project_id: str | None = None
    ) -> list[dict[str, Any]]:
        """合并持久历史与当前 Runtime 的直接及队列执行，先筛选范围再限制数量。

        Args:
            definition_name: 可选逻辑定义范围。
            project_id: 可选项目范围；尚无持久归属的代码图属于默认项目。

        Returns:
            最多五百条按开始时间倒序排列的执行快照。

        Raises:
            NotFoundError: project_id 指定的项目不存在。
        """

        if project_id is not None:
            self.store.project(project_id)
        memberships = self.store.memberships() if project_id is not None else {}
        items = {
            item["id"]: item
            for item in self.store.executions(
                definition_name=definition_name, project_id=project_id
            )
        }
        for execution in self.runtime.executions():
            name = self._definition_name(execution.graph)
            if definition_name is not None and name != definition_name:
                continue
            if (
                project_id is not None
                and memberships.get(name, {}).get("project_id", "default") != project_id
            ):
                continue
            items[execution.id] = self.snapshot(execution)
        return sorted(
            items.values(), key=lambda item: item["started_at"] or "", reverse=True
        )[:500]

    def cancel(self, execution_id: str) -> dict[str, Any]:
        """向当前进程中的执行提交协作式取消。

        Args:
            execution_id: 执行标识。

        Returns:
            accepted（取消请求是否被接受）与当前状态快照。

        Raises:
            NotFoundError: 执行不存在。
            ConflictError: 执行只存在于历史记录，当前进程无法取消。
        """

        handle = self.live_execution(execution_id)
        if handle is None:
            self.store.execution(execution_id)
            raise ConflictError("执行不在当前进程中，无法取消")
        return {"accepted": handle.cancel(), **self.snapshot(handle)}

    def events(self, execution_id: str, after: int = 0) -> list[dict[str, Any]]:
        """按游标读取一批执行观测与诊断记录。

        Args:
            execution_id: 执行标识。
            after: 已消费的观测编号。

        Returns:
            最多五百条带递增 id 的记录。
        """

        return self.store.events(execution_id, after)

    def outputs(self, execution_id: str, offset: int = 0) -> list[Any]:
        """按偏移读取一批已归档的终端输出。

        Args:
            execution_id: 执行标识。
            offset: 从零开始的输出偏移。

        Returns:
            最多五百项编码后的 Output。
        """

        return self.store.outputs(execution_id, offset)

    def _compile(self, definition: GraphDefinition) -> Graph:
        """以 Runtime 当前提供的具名值作为输入 selector 编译并冻结图文档。"""

        return self.nodes.compile(definition, selectors=self.runtime.hooks.provided())

    def _observe(self, event: RuntimeEvent) -> None:
        """保存执行相关的生命周期事实；EXECUTION_FINISHED 的终态取自事件本身。

        Worker 在写入 Execution 终态之前发布 EXECUTION_FINISHED，此时句柄可能仍为
        RUNNING；输出归档交给归档线程在执行真正终止后进行。没有 execution_id 的事件
        没有读取入口，不写入数据库。
        """

        if self._closed or event.execution_id is None:
            return
        if self._diagnostics is not None:
            self._diagnostics.observe(event)
        if event.kind is RuntimeEventKind.EXECUTION_STARTED and event.graph:
            self.store.adopt_definitions((self._definition_name(event.graph),))
        self.store.append_event(
            event.execution_id,
            {
                "kind": event.kind.value,
                "graph": event.graph,
                "node": event.node,
                "execution_id": event.execution_id,
                "status": event.status,
                "error_type": event.error_type,
                "work_id": event.work_id,
                "event_type": event.event_type,
                "attributes": redact_value(encode(dict(event.attributes))),
                "occurred_at": event.occurred_at.isoformat(),
            },
        )
        if event.kind is RuntimeEventKind.EXECUTION_STARTED:
            execution = self.runtime.get_execution(event.execution_id)
            with self._attempts_lock:
                # 同一 Work.id 重投递以相同 ID 开始新 Execution：替换旧快照并清除旧尝试的输出。
                self._attempts[execution.id] = execution
                self.store.restart_execution(self.snapshot(execution))
        elif event.kind is RuntimeEventKind.EXECUTION_FINISHED:
            execution = self.runtime.get_execution(event.execution_id)
            snapshot = self.snapshot(execution)
            snapshot.update(
                status=event.status,
                done=True,
                error_type=event.error_type,
                finished_at=event.occurred_at.isoformat(),
                steps=event.attributes.get("steps", snapshot["steps"]),
            )
            self.store.record_execution(snapshot)
            self._archive_queue.put(execution)

    def _archive_loop(self) -> None:
        """依次等待已发布终态事件的执行真正终止，然后归档输出与最终快照。"""

        while (execution := self._archive_queue.get()) is not None:
            try:
                execution.wait()
                self._archive(execution)
            except Exception:
                _LOGGER.exception("failed to archive execution %s", execution.id)

    def _archive(self, execution: Execution) -> None:
        """通过公开迭代接口逐项归档已终止执行的输出，并写入最终快照。

        输出编码失败记为 output_error，不改变执行终态；已被同 ID 新尝试取代的执行不再写入。
        """

        with self._attempts_lock:
            current = self._attempts.get(execution.id)
            if current is not None and current is not execution:
                return
            snapshot = self.snapshot(execution)
            try:
                for index, output in enumerate(execution):
                    self.store.append_output(execution.id, index, encode(output))
            except Exception as exc:
                if exc is not execution.error:
                    snapshot["output_error"] = redact_text(str(exc))
            self.store.record_execution(snapshot)
            self._attempts.pop(execution.id, None)

    def _run_summary(self) -> dict[str, tuple[int, dict[str, Any] | None]]:
        """汇总每个定义的执行数量与最近一次执行，合并尚未写入历史的 Runtime 执行。"""

        summary: dict[str, tuple[int, dict[str, Any] | None]] = dict(
            self.store.run_summary()
        )
        live = self.runtime.executions()
        recorded = self.store.recorded(item.id for item in live)
        for execution in live:
            name = self._definition_name(execution.graph)
            count, last = summary.get(name, (0, None))
            if execution.id not in recorded:
                summary[name] = (count + 1, last or self.snapshot(execution))
            elif last is not None and last["id"] == execution.id:
                summary[name] = (count, self.snapshot(execution))
        return summary

    def _adopt_code_graphs(self) -> None:
        """在写入路径把尚无归属的代码图归入默认项目，使数据库能校验定义标识全局唯一。"""

        self.store.adopt_definitions(
            name for name in self.runtime.graphs() if name not in self._publications
        )

    def _definition_name(self, graph: str) -> str:
        """返回注册名对应的逻辑定义标识；代码图的定义标识就是注册名。"""

        publication = self._publications.get(graph)
        return graph if publication is None else publication.name

    def _publication_view(self, graph: str) -> dict[str, Any] | None:
        """返回注册名对应的发布信息，代码图为 None。"""

        publication = self._publications.get(graph)
        return None if publication is None else publication.view()

    def _adapter(self, annotation: Any) -> TypeAdapter[Any]:
        """返回端口类型的 TypeAdapter，首次使用时创建并缓存。"""

        adapter = self._adapters.get(annotation)
        if adapter is None:
            adapter = self._adapters.setdefault(annotation, TypeAdapter(annotation))
        return adapter

    def _describe(self, graph: str) -> dict[str, Any]:
        """返回注册图的结构描述；注册名在 Runtime 中不可替换，因此按名称缓存。"""

        description = self._descriptions.get(graph)
        if description is None:
            description = describe_graph(graph, self.runtime.graphs()[graph])
            self._descriptions[graph] = description
        return description

    def _release(self) -> None:
        """按安装逆序卸载观察与诊断，等待归档线程处理完已排队执行，再关闭数据库。"""

        if self._observer is not None:
            self._observer.detach()
        if self._diagnostics is not None:
            self._diagnostics.close()
        if self._archiver.is_alive():
            self._archive_queue.put(None)
            self._archiver.join()
        self.store.close()

    def close(self) -> None:
        """幂等卸载观察并关闭服务数据库，不关闭调用方 Runtime。"""

        if not self._closed:
            self._closed = True
            self._release()

    def __enter__(self) -> GraphService:
        """进入服务作用域，离开时由创建者关闭服务。

        Returns:
            当前服务。
        """

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """关闭服务；不关闭调用方的 Runtime。"""

        self.close()

    def create_app(
        self,
        *,
        token: str | None = None,
        max_request_bytes: int = 1_048_576,
        allowed_hosts: Iterable[str] | None = None,
    ) -> Any:
        """创建包含 RPC 与 Studio 的 ASGI 应用。

        Args:
            token: 可选 Bearer 令牌，保护 API、RPC、SSE 与 OpenAPI。
            max_request_bytes: 单个 HTTP 请求体允许的最大字节数。
            allowed_hosts: 允许的 Host 头主机名，None 只允许本机回环名称；`*` 允许任意主机。

        Returns:
            可由 ASGI 服务器或现有应用挂载的 FastAPI 应用。
        """

        from .http import create_app

        return create_app(
            self,
            token=token,
            max_request_bytes=max_request_bytes,
            allowed_hosts=allowed_hosts,
        )

    def serve(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8000,
        token: str | None = None,
        max_request_bytes: int = 1_048_576,
        allowed_hosts: Iterable[str] | None = None,
    ) -> None:
        """在当前进程启动官方服务入口，阻塞直到服务器退出；返回后不关闭服务，由调用方负责 `close()`。

        监听非回环地址且未设置令牌时记录警告日志。

        Args:
            host: 监听地址，默认仅本机。
            port: HTTP 监听端口。
            token: 可选 API Bearer 令牌。
            max_request_bytes: 单个 HTTP 请求体允许的最大字节数。
            allowed_hosts: 允许的 Host 头主机名；None 使用本机回环名称加上 host。
        """

        import uvicorn

        if host not in LOOPBACK_HOSTS and token is None:
            _LOGGER.warning(
                "graph service listens on non-loopback address %r without a token; "
                "any client that can reach it may run graphs",
                host,
            )
        if allowed_hosts is None:
            allowed_hosts = LOOPBACK_HOSTS | {host}
        uvicorn.run(
            self.create_app(
                token=token,
                max_request_bytes=max_request_bytes,
                allowed_hosts=allowed_hosts,
            ),
            host=host,
            port=port,
        )
