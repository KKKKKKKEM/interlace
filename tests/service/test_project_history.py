"""项目和定义的历史筛选必须先于最近记录数量限制。"""

import json
import sqlite3
from pathlib import Path

import pytest

pytest.importorskip("jsonrpcserver")

from interlace import Runtime
from interlace.service import GraphService


def test_history_migration_and_scoped_queries_include_older_definitions(
    tmp_path: Path,
) -> None:
    """繁忙定义的五百条新记录不能隐藏另一项目的旧记录。

    Args:
        tmp_path: 旧服务数据库所在目录。
    """

    database = tmp_path / "old-history.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE executions(id TEXT PRIMARY KEY, body TEXT)")
        for index in range(502):
            graph = "old.task" if index == 0 else "busy.task"
            connection.execute(
                "INSERT INTO executions VALUES (?, ?)",
                (
                    str(index),
                    json.dumps(
                        {
                            "id": str(index),
                            "graph": graph,
                            "done": True,
                            "status": "succeeded",
                            "steps": 1,
                            "started_at": "2026-09-01T00:00:00Z",
                        }
                    ),
                ),
            )
    with Runtime() as runtime:
        service = GraphService(runtime, database=database)
        project = service.store.save_project("历史项目", "")
        assert "old.task" in service.store.memberships()
        service.store.assign_definition("old.task", project["id"])
        assert len(service.executions()) == 500
        assert service.definition("old.task")["runs"][0]["id"] == "0"
        assert service.executions(project_id=project["id"])[0]["id"] == "0"
        assert service.definitions(project["id"])[0]["last_run"]["id"] == "0"
        assert (
            service.store.connection.execute(
                "SELECT COUNT(*) FROM executions"
            ).fetchone()[0]
            == 502
        )
        service.close()
