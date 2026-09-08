"""项目、逻辑定义与版本和运行记录的管理契约。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("jsonrpcserver")

from fastapi.testclient import TestClient

from interlace import Graph, Node, Output, Runtime
from interlace.service import GraphService, NodeCatalog


class Echo(Node):
    """项目管理测试中的可编辑节点。"""

    def execute(self, inputs: Any, context: Any) -> Output:
        """保留输入值，供跨项目管理后的执行验证。

        Args:
            inputs: default 端口输入。
            context: 当前执行上下文。

        Returns:
            原始输入值。
        """

        del context
        return Output(inputs["default"])


def document(name: str) -> dict[str, Any]:
    """创建一个可发布的逻辑定义文档。

    Args:
        name: 全局定义标识。

    Returns:
        单节点 JSON 文档。
    """

    return {
        "name": name,
        "entrypoint": "echo",
        "nodes": [{"id": "echo", "type": "echo", "config": {}}],
        "edges": [],
    }


@pytest.fixture
def api() -> Iterator[tuple[TestClient, GraphService]]:
    """建立带代码图与编辑目录的独立服务。

    Yields:
        管理接口客户端与服务实例。
    """

    with Runtime() as runtime:
        runtime.register("code.task", Graph(entrypoint="echo").add(echo=Echo()))
        service = GraphService(runtime, nodes=NodeCatalog().register_node("echo", Echo))
        with TestClient(service.create_app()) as client:
            yield client, service


def test_default_project_adopts_code_and_dynamic_registrations(api: Any) -> None:
    """已有和新注册代码图只在首次发现时归入默认项目。

    Args:
        api: 管理测试客户端与服务。
    """

    client, service = api
    projects = client.get("/api/projects").json()
    assert projects[0]["id"] == "default"
    assert projects[0]["definition_count"] == 1
    service.runtime.register("new.task", Graph(entrypoint="echo").add(echo=Echo()))
    definitions = client.get("/api/projects/default/definitions").json()["definitions"]
    assert {item["name"] for item in definitions} == {"code.task", "new.task"}
    assert client.get("/api/projects/missing/definitions").status_code == 404


def test_versions_are_one_definition_and_project_runs_are_scoped(api: Any) -> None:
    """同一定义的两个版本合并展示，运行记录按项目归属过滤。

    Args:
        api: 管理测试客户端与服务。
    """

    client, service = api
    project = client.post(
        "/api/projects", json={"name": "采集项目", "description": "业务定义"}
    ).json()
    project_id = project["id"]
    created = client.post(
        f"/api/projects/{project_id}/definitions", json=document("daily")
    )
    assert created.status_code == 201
    assert created.json()["draft"]["project_id"] == project_id
    assert created.json()["draft"]["revision"] == 1
    for revision in (1, 2):
        response = client.post(
            "/api/publish",
            json={
                "definition": document("daily"),
                "revision": revision,
                "project_id": project_id,
            },
        )
        assert response.status_code == 200
    call = client.post(
        "/api/call", json={"graph": "daily@v1", "inputs": {"default": "value"}}
    ).json()
    assert call["outputs"][0]["value"] == "value"
    service.runtime.wait_idle()
    rows = client.get(f"/api/projects/{project_id}/definitions").json()["definitions"]
    assert len(rows) == 1
    assert rows[0]["version"] == 2 and rows[0]["name"] == "daily"
    detail = client.get("/api/definitions/daily").json()
    assert len(detail["graphs"]) == 2
    assert detail["runs"][0]["id"] == call["id"]
    assert client.get("/api/projects/default/executions").json() == []
    assert (
        client.get(f"/api/projects/{project_id}/executions").json()[0][
            "definition_name"
        ]
        == "daily"
    )


def test_move_preserves_definition_versions_history_and_runtime_names(api: Any) -> None:
    """移动定义只改变管理归属，旧版本仍能执行且历史跟随整条定义。

    Args:
        api: 管理测试客户端与服务。
    """

    client, service = api
    source = client.post("/api/projects", json={"name": "源项目"}).json()["id"]
    target = client.post("/api/projects", json={"name": "目标项目"}).json()["id"]
    client.post(f"/api/projects/{source}/definitions", json=document("move.task"))
    client.post(
        "/api/publish",
        json={"definition": document("move.task"), "revision": 1, "project_id": source},
    )
    run = client.post(
        "/api/call", json={"graph": "move.task@v1", "inputs": {"default": 7}}
    ).json()
    service.runtime.wait_idle()
    result = client.post(
        "/api/definition-project", json={"name": "move.task", "project_id": target}
    )
    assert result.status_code == 200
    assert result.json()["project_id"] == target
    assert result.json()["draft"]["project_id"] == target
    assert (
        client.get(f"/api/definitions/move.task?project_id={source}").status_code == 404
    )
    assert (
        client.get(f"/api/definitions/move.task?project_id={target}").status_code == 200
    )
    assert client.get(f"/api/projects/{source}/definitions").json()["definitions"] == []
    assert client.get(f"/api/projects/{target}/executions").json()[0]["id"] == run["id"]
    assert service.runtime.run("move.task@v1", 8)[0].value == 8
    service.runtime.wait_idle()
    assert (
        client.post(
            "/api/definition-project", json={"name": "missing", "project_id": target}
        ).status_code
        == 404
    )


def test_cross_project_writes_and_duplicate_definition_creation_are_rejected(
    api: Any,
) -> None:
    """项目参数错误不能隐式移动定义或覆盖同名代码图。

    Args:
        api: 管理测试客户端与服务。
    """

    client, _ = api
    project = client.post("/api/projects", json={"name": "独立项目"}).json()["id"]
    assert (
        client.post(
            f"/api/projects/{project}/definitions", json=document("code.task")
        ).status_code
        == 409
    )
    assert (
        client.post(
            "/api/drafts",
            json={"definition": document("code.task"), "project_id": project},
        ).status_code
        == 409
    )
    client.post(f"/api/projects/{project}/definitions", json=document("draft.task"))
    assert (
        client.post(
            "/api/publish", json={"definition": document("draft.task"), "revision": 1}
        ).status_code
        == 409
    )
    assert client.get("/api/definitions/draft.task").json()["draft"]["revision"] == 1
    assert (
        client.post(
            "/api/drafts",
            json={"definition": document("unknown.project"), "project_id": "missing"},
        ).status_code
        == 404
    )


def test_project_settings_and_empty_delete(api: Any) -> None:
    """项目可改名与修改描述，默认和非空项目不能被删除。

    Args:
        api: 管理测试客户端与服务。
    """

    client, _ = api
    project = client.post("/api/projects", json={"name": "临时项目"}).json()["id"]
    result = client.post(
        f"/api/projects/{project}", json={"name": "新名称", "description": "新的用途"}
    )
    assert result.json()["id"] == project
    assert result.json()["description"] == "新的用途"
    assert client.post("/api/projects", json={"name": "新名称"}).status_code == 409
    assert client.post("/api/projects", json={"name": "   "}).status_code == 422
    assert client.delete("/api/projects/default").status_code == 409
    assert client.delete(f"/api/projects/{project}").status_code == 200
    assert client.get(f"/api/projects/{project}/definitions").status_code == 404


def test_existing_database_migrates_without_rewriting_definitions_or_history(
    tmp_path: Path,
) -> None:
    """旧数据库一次性迁入默认项目，后续重启保留用户调整的归属。

    Args:
        tmp_path: 用于生成旧版本服务数据库的临时目录。
    """

    database = tmp_path / "legacy.sqlite3"
    body = json.dumps(document("legacy"))
    with sqlite3.connect(database) as connection:
        connection.executescript(
            "CREATE TABLE documents(name TEXT PRIMARY KEY, revision INTEGER, body TEXT); CREATE TABLE versions(name TEXT, version INTEGER, body TEXT, PRIMARY KEY(name,version)); CREATE TABLE executions(id TEXT PRIMARY KEY, body TEXT);"
        )
        connection.execute("INSERT INTO documents VALUES ('legacy', 7, ?)", (body,))
        connection.execute("INSERT INTO versions VALUES ('legacy', 1, ?)", (body,))
        connection.execute(
            "INSERT INTO executions VALUES ('old', ?)",
            (
                json.dumps(
                    {
                        "id": "old",
                        "graph": "legacy@v1",
                        "done": True,
                        "status": "succeeded",
                        "steps": 1,
                        "started_at": "2026-09-01T00:00:00Z",
                    }
                ),
            ),
        )
    with Runtime() as runtime:
        service = GraphService(
            runtime, nodes=NodeCatalog().register_node("echo", Echo), database=database
        )
        assert (
            service.store.connection.execute("PRAGMA user_version").fetchone()[0] == 2
        )
        assert service.definitions("default")[0]["name"] == "legacy"
        assert (
            service.store.connection.execute("SELECT body FROM versions").fetchone()[0]
            == body
        )
        assert service.store.drafts()[0]["revision"] == 7
        assert service.definition("legacy")["runs"][0]["id"] == "old"
        project_id = service.store.save_project("迁移后的项目", "")["id"]
        service.store.assign_definition("legacy", project_id)
        service.close()
    with Runtime() as runtime:
        service = GraphService(
            runtime, nodes=NodeCatalog().register_node("echo", Echo), database=database
        )
        assert service.definitions("default") == []
        assert service.definition("legacy")["project_id"] == project_id
        assert runtime.run("legacy@v1", "still works")[0].value == "still works"
        runtime.wait_idle()
        service.close()
