"""协议与工作台共用的官方图服务。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter
from pydantic_core import to_jsonable_python

from interlace import Execution, Runtime
from interlace.engine.observation import RuntimeEvent

from .catalog import NodeCatalog, describe_graph
from .diagnostics import NodeDiagnostics, snapshot_value
from .models import DraftRequest, GraphDefinition, RunRequest
from .store import ConflictError, ServiceStore


def encode(value: Any) -> Any:
    """将支持的 Python 值编码为严格 JSON；不使用 repr 隐式降级。

    Args:
        value: 内置值、数据类或具有 Pydantic 序列化规则的对象。

    Returns:
        可无损表示于 JSON 的结构。

    Raises:
        ValueError: 对象无法通过声明的序列化规则编码。
    """

    try:
        result = to_jsonable_python(value, serialize_unknown=False)
        json.dumps(result, allow_nan=False)
        return result
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"output is not JSON serializable: {type(value).__name__}"
        ) from exc


class GraphService:
    """按需启用 RPC 和 Studio，共用调用方的 Runtime。

    Attributes:
        runtime: 调用方管理生命周期的运行时。
        nodes: 显式登记的编辑器节点目录。
        store: 当前服务独占的文档和观测数据库。
        _observer: 服务持有的只读观察注册。
        _closed: 服务是否已关闭。
        _diagnostics: 节点参数与日志采集器，关闭采集时为 None。
    """

    def __init__(
        self,
        runtime: Runtime,
        *,
        nodes: NodeCatalog | None = None,
        database: str | Path = ":memory:",
        diagnostics: bool = True,
    ) -> None:
        """接入已有 Runtime，并恢复已发布的图定义。

        Args:
            runtime: 已有运行时，必须提供公开 GraphCatalog 能力。
            nodes: 编辑器可构造的节点类型目录。
            database: 服务数据库路径，默认仅内存保存。
            diagnostics: 是否采集实际节点输入、原始输出和 Python 日志，默认启用。

        Raises:
            ValueError: 已保存版本无法由当前节点目录编译。
            TypeError: Worker 不支持公开图目录。
        """

        runtime.graphs()
        self.runtime = runtime
        self.nodes = nodes or NodeCatalog()
        self.store = ServiceStore(database)
        self._closed = False
        self._diagnostics: NodeDiagnostics | None = None
        try:
            versions = self.store.versions()
            compiled = [
                (
                    f"{item['name']}@v{item['version']}",
                    self.nodes.compile(
                        GraphDefinition.model_validate(item["definition"])
                    ),
                )
                for item in versions
            ]
            if any(name in runtime.graphs() for name, _ in compiled):
                raise ConflictError(
                    "saved graph version conflicts with Runtime registration"
                )
            for name, graph in compiled:
                runtime.register(name, graph)
            names = {
                f"{item['name']}@v{item['version']}": item["name"] for item in versions
            }
            self.store.adopt_definitions(
                [names.get(name, name) for name in runtime.graphs()]
            )
            for snapshot in self.store.executions():
                if not snapshot["done"]:
                    snapshot.update(
                        status="interrupted", done=True, error="服务已重启，运行未恢复"
                    )
                    self.store.record_execution(snapshot)
            if diagnostics:
                self._diagnostics = NodeDiagnostics(runtime, self.store)
            self._observer = runtime.observe_runtime(self._observe)
        except BaseException:
            if self._diagnostics is not None:
                self._diagnostics.close()
            self.store.close()
            raise

    def graphs(self) -> list[dict[str, Any]]:
        """读取代码图与已发布版本，附带可编辑文档。

        Returns:
            图结构、注册名称与可选版本文档。
        """

        versions = {
            f"{item['name']}@v{item['version']}": item for item in self.store.versions()
        }
        return [
            {**describe_graph(name, graph), "publication": versions.get(name)}
            for name, graph in self.runtime.graphs().items()
        ]

    def validate(self, definition: GraphDefinition) -> dict[str, Any]:
        """构造并冻结草稿以执行核心校验，不发布版本。

        Args:
            definition: 待校验文档。

        Returns:
            冻结后的真实端口与图结构。
        """

        return describe_graph(definition.name, self.nodes.compile(definition))

    def definitions(self, project_id: str | None = None) -> list[dict[str, Any]]:
        """按逻辑定义合并草稿、不可变版本及代码注册。

        Args:
            project_id: 限定项目，None 返回全部管理定义。

        Returns:
            适合任务管理列表的定义摘要，不把每个发布版本重复显示为一行。
        """

        if project_id is not None:
            self.store.project(project_id)
        graphs = self.graphs()
        versions = self.store.versions()
        names = {
            f"{item['name']}@v{item['version']}": item["name"] for item in versions
        }
        runs = self.executions()
        self.store.adopt_definitions(
            list(
                {names.get(graph["name"], graph["name"]) for graph in graphs}
                | {
                    names.get(run["graph"], run["graph"])
                    for run in runs
                    if "graph" in run
                }
            )
        )
        drafts = {item["definition"]["name"]: item for item in self.store.drafts()}
        rows = []
        for name, membership in self.store.memberships().items():
            if project_id is not None and membership["project_id"] != project_id:
                continue
            published = [
                item
                for item in graphs
                if item["publication"] and item["publication"]["name"] == name
            ]
            published.sort(
                key=lambda item: item["publication"]["version"], reverse=True
            )
            code = next(
                (
                    item
                    for item in graphs
                    if item["name"] == name and not item["publication"]
                ),
                None,
            )
            latest = published[0] if published else code
            draft = drafts.get(name)
            related_runs = self.executions(definition_name=name)
            rows.append(
                {
                    "name": name,
                    **membership,
                    "source": "visual" if draft or published else "code",
                    "status": "published"
                    if published
                    else "code"
                    if code
                    else "draft"
                    if draft
                    else "unavailable",
                    "graph": latest["name"] if latest else None,
                    "version": published[0]["publication"]["version"]
                    if published
                    else None,
                    "draft_revision": draft["revision"] if draft else None,
                    "node_count": len(latest["nodes"])
                    if latest
                    else len(draft["definition"]["nodes"])
                    if draft
                    else 0,
                    "last_run": related_runs[0] if related_runs else None,
                    "run_count": len(related_runs),
                }
            )
        return sorted(rows, key=lambda row: row["name"])

    def projects(self) -> list[dict[str, Any]]:
        """读取项目及当前定义数量与运行状态。

        Returns:
            项目清单与各项目的可见定义统计。
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

    def definition(self, name: str) -> dict[str, Any]:
        """取得单条定义的管理详情、版本、草稿与运行记录。

        Args:
            name: 定义的全局标识。

        Returns:
            可直接驱动定义详情页面的结构化数据。

        Raises:
            KeyError: 定义不存在。
        """

        summary = next(
            (item for item in self.definitions() if item["name"] == name), None
        )
        if summary is None:
            raise KeyError(name)
        graphs = [
            item
            for item in self.graphs()
            if item["name"] == name
            or (item["publication"] and item["publication"]["name"] == name)
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
            "draft": next(
                (
                    item
                    for item in self.store.drafts()
                    if item["definition"]["name"] == name
                ),
                None,
            ),
            "runs": self.executions(definition_name=name),
        }

    def publish(self, request: DraftRequest) -> dict[str, Any]:
        """发布新版本并固定运行注册名称，保持历史版本不变。

        Args:
            request: 基于当前草稿修订号的完整图文档。

        Returns:
            新版本、运行名称及更新后的草稿修订号。

        Raises:
            ConflictError: 草稿修订号已过期。
            GraphError: 图未通过冻结校验。
        """

        self.definitions()
        graph = self.nodes.compile(request.definition)
        name = request.definition.name
        body = request.definition.model_dump()
        with self.store.lock:
            self.store.require_definition_project(name, request.project_id)
            current = next(
                (d for d in self.store.drafts() if d["definition"]["name"] == name),
                None,
            )
            if (0 if current is None else current["revision"]) != request.revision:
                raise ConflictError("草稿已更新，请重新打开后合并修改")
            versions = [
                v["version"] for v in self.store.versions() if v["name"] == name
            ]
            version = max(versions, default=0) + 1
            registered = f"{name}@v{version}"
            with self.store.connection:
                self.store.connection.execute(
                    "INSERT INTO versions VALUES (?, ?, ?)",
                    (
                        name,
                        version,
                        json.dumps(body, ensure_ascii=False, allow_nan=False),
                    ),
                )
                self.runtime.register(registered, graph)
            revision = self.store.save_draft(
                name, body, request.revision, request.project_id
            )
        return {"graph": registered, "version": version, "revision": revision}

    def start(self, request: RunRequest) -> Execution:
        """按端口类型解码 JSON 并提交统一 Execution。

        Args:
            request: 协议无关的执行参数，inputs 始终按端口命名。

        Returns:
            可等待、取消和迭代的原生执行句柄。

        Raises:
            KeyError: 图未注册。
            ValueError: 输入端口或值不符合传输契约。
        """

        if self._closed:
            raise RuntimeError("graph service is closed")
        graph = self.runtime.graphs()[request.graph]
        ports = graph.spec_for(graph.entrypoint).input_ports
        if set(request.inputs) != set(ports):
            raise ValueError(f"inputs must contain exactly these ports: {list(ports)}")
        inputs = {
            port: TypeAdapter(typ).validate_json(
                json.dumps(request.inputs[port]), strict=True
            )
            for port, typ in ports.items()
        }
        execution = self.runtime.start(
            request.graph,
            next(iter(inputs.values())) if len(ports) == 1 else inputs,
            options=request.options,
            max_steps=request.max_steps,
            timeout=request.timeout,
        )
        self.store.record_execution(self.snapshot(execution))
        if execution.done:
            self._archive(execution)
        return execution

    def snapshot(self, execution: Execution) -> dict[str, Any]:
        """读取执行状态，不物化完整输出。

        Args:
            execution: 当前进程的执行句柄。

        Returns:
            可用于列表和历史记录的状态快照。
        """

        publication = next(
            (
                item
                for item in self.store.versions()
                if f"{item['name']}@v{item['version']}" == execution.graph
            ),
            None,
        )
        return {
            "id": execution.id,
            "graph": execution.graph,
            "node_configs": {
                item["id"]: snapshot_value(item["config"])
                for item in publication["definition"]["nodes"]
            }
            if publication
            else {},
            "diagnostics": self._diagnostics is not None,
            "graph_snapshot": describe_graph(
                execution.graph, self.runtime.graphs()[execution.graph]
            ),
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
            "error": str(execution.error) if execution.error else None,
            "cancel_requested": execution.cancel_requested,
        }

    def execution(self, execution_id: str) -> dict[str, Any]:
        """优先读取当前句柄，已经离开进程的执行读取历史快照。

        Args:
            execution_id: 目标执行标识。

        Returns:
            执行状态快照。

        Raises:
            KeyError: 当前与历史都没有此执行。
        """

        live = {item.id: item for item in self.runtime.executions()}
        if execution_id in live:
            return self.snapshot(live[execution_id])
        return self.store.execution(execution_id)

    def executions(
        self, *, definition_name: str | None = None, project_id: str | None = None
    ) -> list[dict[str, Any]]:
        """合并持久历史和当前 Runtime 的直接及队列执行。

        Args:
            definition_name: 可选逻辑定义范围。
            project_id: 可选项目范围。

        Returns:
            最多五百条按开始时间逆序排列的执行状态。
        """

        names = {
            f"{item['name']}@v{item['version']}": item["name"]
            for item in self.store.versions()
        }
        memberships = self.store.memberships()
        items = {
            item["id"]: item
            for item in self.store.executions(
                definition_name=definition_name, project_id=project_id
            )
        }
        items.update(
            {
                item.id: self.snapshot(item)
                for item in self.runtime.executions()
                if (
                    definition_name is None
                    or names.get(item.graph, item.graph) == definition_name
                )
                and (
                    project_id is None
                    or memberships.get(names.get(item.graph, item.graph), {}).get(
                        "project_id"
                    )
                    == project_id
                )
            }
        )
        return sorted(
            items.values(), key=lambda item: item["started_at"] or "", reverse=True
        )[:500]

    def _observe(self, event: RuntimeEvent) -> None:
        """保存不含业务值的运行事件，采集失败由 Runtime 隔离。

        Args:
            event: Runtime 发布的只读生命周期事实。
        """

        if self._closed:
            return
        if event.kind.value == "execution.started" and event.graph is not None:
            names = {
                f"{item['name']}@v{item['version']}": item["name"]
                for item in self.store.versions()
            }
            self.store.adopt_definitions([names.get(event.graph, event.graph)])
        if self._diagnostics is not None:
            self._diagnostics.observe(event)
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
                "attributes": encode(dict(event.attributes)),
                "occurred_at": event.occurred_at.isoformat(),
            },
        )
        if event.execution_id and event.kind.value.startswith("execution."):
            execution = self.runtime.get_execution(event.execution_id)
            self.store.record_execution(self.snapshot(execution))
            if execution.done:
                self._archive(execution)

    def _archive(self, execution: Execution) -> None:
        """通过公开迭代接口分项归档终态输出，不改变业务结果。

        Args:
            execution: 已经终止的执行句柄。
        """

        snapshot = self.snapshot(execution)
        try:
            for index, output in enumerate(execution):
                self.store.append_output(execution.id, index, encode(output))
        except Exception as exc:
            if exc is not execution.error:
                snapshot["output_error"] = str(exc)
        self.store.record_execution(snapshot)

    def close(self) -> None:
        """卸载观察并关闭服务数据库，不关闭调用方 Runtime。"""

        if not self._closed:
            self._closed = True
            self._observer.detach()
            if self._diagnostics is not None:
                self._diagnostics.close()
            self.store.close()

    def create_app(self, *, token: str | None = None) -> Any:
        """创建包含 RPC 与 Studio 的 ASGI 应用。

        Args:
            token: 可选 Bearer 令牌，保护 API 和执行数据。

        Returns:
            可由 ASGI 服务器或现有应用挂载的 FastAPI 应用。
        """

        from .http import create_app

        return create_app(self, token=token)

    def serve(
        self, *, host: str = "127.0.0.1", port: int = 8000, token: str | None = None
    ) -> None:
        """在当前进程启动官方服务入口。

        Args:
            host: 监听地址，默认仅本机。
            port: HTTP 监听端口。
            token: 可选 API Bearer 令牌。
        """

        import uvicorn

        uvicorn.run(self.create_app(token=token), host=host, port=port)
