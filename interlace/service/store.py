"""服务层 SQLite 文档和观测历史；不承担任务恢复或消息交付。"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4


class ConflictError(ValueError):
    """草稿已被其他编辑者修改，调用方应重新读取后合并。"""


class ServiceStore:
    """将草稿、发布版本和运行观测保存到本地数据库。

    Attributes:
        lock: 串行化数据库事务，也用于服务发布操作。
        connection: 当前实例独占的 SQLite 连接。
        _closed: 连接是否已关闭。
    """

    def __init__(self, path: str | Path = ":memory:") -> None:
        """打开数据库并建立服务自己的表。

        Args:
            path: 数据库文件，默认只在内存中保留。
        """

        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = RLock()
        self._closed = False
        self.connection = sqlite3.connect(str(path), check_same_thread=False)
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS documents (
                name TEXT PRIMARY KEY, revision INTEGER NOT NULL, body TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS versions (
                name TEXT NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL,
                PRIMARY KEY (name, version)
            );
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY, body TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT, body TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS event_execution ON events(execution_id, id);
            CREATE TABLE IF NOT EXISTS outputs (
                execution_id TEXT NOT NULL, ordinal INTEGER NOT NULL, body TEXT NOT NULL,
                PRIMARY KEY (execution_id, ordinal)
            );
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS definition_projects (
                name TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(id),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE INDEX IF NOT EXISTS project_definitions ON definition_projects(project_id);
            """
        )
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO projects(id, name, description) VALUES ('default', '默认项目', '')"
            )
            if self.connection.execute("PRAGMA user_version").fetchone()[0] == 0:
                # 将已有草稿与发布版本一次性归入默认项目，保持原运行注册名与历史数据。
                self.connection.execute(
                    "INSERT OR IGNORE INTO definition_projects(name, project_id) "
                    "SELECT name, 'default' FROM documents UNION SELECT name, 'default' FROM versions"
                )
                self.connection.execute("PRAGMA user_version=1")
            if self.connection.execute("PRAGMA user_version").fetchone()[0] < 2:
                self.connection.execute("ALTER TABLE executions ADD COLUMN graph TEXT")
                names = {
                    f"{name}@v{version}": name
                    for name, version in self.connection.execute(
                        "SELECT name, version FROM versions"
                    )
                }
                # 全量建立查询索引，不让其他项目的近期运行挤掉旧项目历史。
                for execution_id, body in self.connection.execute(
                    "SELECT id, body FROM executions"
                ):
                    graph = json.loads(body).get("graph")
                    if isinstance(graph, str):
                        self.connection.execute(
                            "UPDATE executions SET graph=? WHERE id=?",
                            (graph, execution_id),
                        )
                        self.connection.execute(
                            "INSERT OR IGNORE INTO definition_projects(name, project_id) VALUES (?, 'default')",
                            (names.get(graph, graph),),
                        )
                self.connection.execute(
                    "CREATE INDEX IF NOT EXISTS execution_graph ON executions(graph)"
                )
                self.connection.execute("PRAGMA user_version=2")

    def projects(self) -> list[dict[str, Any]]:
        """读取持久项目清单。

        Returns:
            项目标识、名称、描述和创建更新时间。
        """

        with self.lock:
            return [
                dict(
                    zip(("id", "name", "description", "created_at", "updated_at"), row)
                )
                for row in self.connection.execute(
                    "SELECT id, name, description, created_at, updated_at FROM projects ORDER BY created_at, id"
                )
            ]

    def project(self, project_id: str) -> dict[str, Any]:
        """按标识读取项目，未知项目明确失败。

        Args:
            project_id: 项目稳定标识。

        Returns:
            项目元数据。

        Raises:
            KeyError: 项目不存在。
        """

        for project in self.projects():
            if project["id"] == project_id:
                return project
        raise KeyError(project_id)

    def save_project(
        self, name: str, description: str, project_id: str | None = None
    ) -> dict[str, Any]:
        """创建项目或修改显示信息，保留稳定标识。

        Args:
            name: 项目显示名称。
            description: 项目描述。
            project_id: 已有项目标识，None 创建新项目。

        Returns:
            保存后的项目元数据。

        Raises:
            ValueError: 名称为空。
            ConflictError: 名称已被使用。
            KeyError: 更新目标不存在。
        """

        name = name.strip()
        if not name:
            raise ValueError("项目名称不能为空")
        with self.lock, self.connection:
            if project_id is not None:
                self.project(project_id)
            if self.connection.execute(
                "SELECT 1 FROM projects WHERE name=? AND id<>?",
                (name, project_id or ""),
            ).fetchone():
                raise ConflictError("项目名称已存在")
            if project_id is None:
                project_id = str(uuid4())
                self.connection.execute(
                    "INSERT INTO projects(id, name, description) VALUES (?, ?, ?)",
                    (project_id, name, description),
                )
            else:
                self.connection.execute(
                    "UPDATE projects SET name=?, description=?, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id=?",
                    (name, description, project_id),
                )
        return self.project(project_id)

    def delete_project(self, project_id: str) -> None:
        """删除不含定义的非默认项目。

        Args:
            project_id: 待删除项目。

        Raises:
            ConflictError: 项目仍有定义或属于默认项目。
            KeyError: 项目不存在。
        """

        with self.lock, self.connection:
            self.project(project_id)
            if (
                project_id == "default"
                or self.connection.execute(
                    "SELECT 1 FROM definition_projects WHERE project_id=?",
                    (project_id,),
                ).fetchone()
            ):
                raise ConflictError("默认项目或仍包含定义的项目不能删除")
            self.connection.execute("DELETE FROM projects WHERE id=?", (project_id,))

    def memberships(self) -> dict[str, dict[str, Any]]:
        """读取定义到项目的管理归属。

        Returns:
            按定义全局标识索引的项目与更新时间。
        """

        with self.lock:
            return {
                name: {"project_id": project_id, "updated_at": updated_at}
                for name, project_id, updated_at in self.connection.execute(
                    "SELECT name, project_id, updated_at FROM definition_projects"
                )
            }

    def adopt_definitions(self, names: list[str]) -> None:
        """将首次发现的代码定义归入默认项目，已有归属保持不变。

        Args:
            names: 运行时发现的逻辑定义标识。
        """

        with self.lock, self.connection:
            self.connection.executemany(
                "INSERT OR IGNORE INTO definition_projects(name, project_id) VALUES (?, 'default')",
                [(name,) for name in names],
            )

    def assign_definition(self, name: str, project_id: str) -> None:
        """移动定义的管理归属，不修改版本内容或运行注册名称。

        Args:
            name: 已知定义标识。
            project_id: 目标项目标识。

        Raises:
            KeyError: 项目或定义不存在。
        """

        with self.lock, self.connection:
            self.project(project_id)
            if name not in self.memberships():
                raise KeyError(name)
            self.connection.execute(
                "UPDATE definition_projects SET project_id=?, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE name=?",
                (project_id, name),
            )

    def require_definition_project(self, name: str, project_id: str) -> None:
        """校验保存目标项目与已有定义归属一致。

        Args:
            name: 定义全局标识。
            project_id: 本次保存指定的项目。

        Raises:
            ConflictError: 标识已属于其他项目。
            KeyError: 项目不存在。
        """

        self.project(project_id)
        membership = self.memberships().get(name)
        if membership and membership["project_id"] != project_id:
            raise ConflictError("定义标识已属于其他项目，请使用其他标识或先移动定义")

    def drafts(self) -> list[dict[str, Any]]:
        """读取全部草稿修订快照。

        Returns:
            按名称排列的草稿和修订号。
        """

        with self.lock:
            return [
                {
                    "revision": revision,
                    "definition": json.loads(body),
                    "project_id": project_id,
                }
                for revision, body, project_id in self.connection.execute(
                    "SELECT d.revision, d.body, p.project_id FROM documents d JOIN definition_projects p ON p.name=d.name ORDER BY d.name"
                )
            ]

    def save_draft(
        self,
        name: str,
        body: dict[str, Any],
        revision: int,
        project_id: str = "default",
    ) -> int:
        """比较修订号后原子保存草稿。

        Args:
            name: 图文档名称。
            body: 完整 JSON 文档。
            revision: 调用方基于的修订号。
            project_id: 所属项目，默认进入默认项目。

        Returns:
            新修订号。

        Raises:
            ConflictError: 文档已被修改。
        """

        with self.lock, self.connection:
            self.require_definition_project(name, project_id)
            current = self.connection.execute(
                "SELECT revision FROM documents WHERE name=?", (name,)
            ).fetchone()
            if (0 if current is None else current[0]) != revision:
                raise ConflictError("草稿已更新，请重新打开后合并修改")
            self.connection.execute(
                "INSERT OR REPLACE INTO documents VALUES (?, ?, ?)",
                (
                    name,
                    revision + 1,
                    json.dumps(body, ensure_ascii=False, allow_nan=False),
                ),
            )
            self.connection.execute(
                "INSERT INTO definition_projects(name, project_id) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
                (name, project_id),
            )
        return revision + 1

    def versions(self) -> list[dict[str, Any]]:
        """读取不可变发布版本。

        Returns:
            名称、版本号及原始图文档。
        """

        with self.lock:
            return [
                {"name": name, "version": version, "definition": json.loads(body)}
                for name, version, body in self.connection.execute(
                    "SELECT name, version, body FROM versions ORDER BY name, version"
                )
            ]

    def record_execution(self, snapshot: dict[str, Any]) -> None:
        """保存一个执行的最新可序列化状态。

        Args:
            snapshot: 包含 id 的执行状态快照。
        """

        with self.lock:
            if self._closed:
                return
            with self.connection:
                current = self.connection.execute(
                    "SELECT body FROM executions WHERE id=?", (snapshot["id"],)
                ).fetchone()
                if (
                    current is not None
                    and json.loads(current[0])["done"]
                    and not snapshot["done"]
                ):
                    return
                self.connection.execute(
                    "INSERT OR REPLACE INTO executions(id, body, graph) VALUES (?, ?, ?)",
                    (
                        snapshot["id"],
                        json.dumps(snapshot, ensure_ascii=False, allow_nan=False),
                        snapshot.get("graph"),
                    ),
                )

    def executions(
        self, *, definition_name: str | None = None, project_id: str | None = None
    ) -> list[dict[str, Any]]:
        """读取历史执行快照。

        Args:
            definition_name: 可选逻辑定义范围，包含其全部发布版本。
            project_id: 可选项目范围，在应用数量上限之前筛选。

        Returns:
            按最近写入顺序排列的执行快照。
        """

        conditions = []
        parameters: list[str] = []
        if definition_name is not None:
            conditions.append(
                "(graph=? OR graph IN (SELECT name || '@v' || version FROM versions WHERE name=?))"
            )
            parameters.extend((definition_name, definition_name))
        if project_id is not None:
            conditions.append(
                "graph IN (SELECT name FROM definition_projects WHERE project_id=? UNION SELECT v.name || '@v' || v.version FROM versions v JOIN definition_projects p ON p.name=v.name WHERE p.project_id=?)"
            )
            parameters.extend((project_id, project_id))
        query = "SELECT body FROM executions"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        with self.lock:
            return [
                json.loads(row[0])
                for row in self.connection.execute(
                    query + " ORDER BY rowid DESC LIMIT 500", parameters
                )
            ]

    def execution(self, execution_id: str) -> dict[str, Any]:
        """按标识读取历史快照。

        Args:
            execution_id: 执行标识。

        Returns:
            已保存的状态。

        Raises:
            KeyError: 没有此记录。
        """

        with self.lock:
            row = self.connection.execute(
                "SELECT body FROM executions WHERE id=?", (execution_id,)
            ).fetchone()
        if row is None:
            raise KeyError(execution_id)
        return json.loads(row[0])

    def append_event(self, execution_id: str | None, body: dict[str, Any]) -> None:
        """追加一条只读观测记录。

        Args:
            execution_id: 关联执行，外部事件允许为 None。
            body: 不携带业务值的生命周期元数据。
        """

        with self.lock:
            if self._closed:
                return
            with self.connection:
                self.connection.execute(
                    "INSERT INTO events(execution_id, body) VALUES (?, ?)",
                    (
                        execution_id,
                        json.dumps(body, ensure_ascii=False, allow_nan=False),
                    ),
                )

    def events(self, execution_id: str, after: int = 0) -> list[dict[str, Any]]:
        """按追加顺序分页读取观测事件，游标可用于重连。

        Args:
            execution_id: 目标执行标识。
            after: 已消费的事件编号，默认从头读取。

        Returns:
            最多五百条带递增 id 的事件。
        """

        with self.lock:
            return [
                {"id": event_id, **json.loads(body)}
                for event_id, body in self.connection.execute(
                    "SELECT id, body FROM events WHERE execution_id=? AND id>? ORDER BY id LIMIT 500",
                    (execution_id, after),
                )
            ]

    def append_output(self, execution_id: str, ordinal: int, output: Any) -> None:
        """按稳定序号保存已结束执行的可编码终端输出。

        Args:
            execution_id: 执行标识。
            ordinal: 从零开始的追加序号。
            output: 已编码的 Output。
        """

        with self.lock:
            if self._closed:
                return
            with self.connection:
                self.connection.execute(
                    "INSERT OR IGNORE INTO outputs VALUES (?, ?, ?)",
                    (
                        execution_id,
                        ordinal,
                        json.dumps(output, ensure_ascii=False, allow_nan=False),
                    ),
                )

    def outputs(self, execution_id: str, offset: int = 0) -> list[Any]:
        """按追加顺序分批读取历史输出，不物化全部结果。

        Args:
            execution_id: 执行标识。
            offset: 从零开始的输出偏移。

        Returns:
            最多五百项编码后的 Output。
        """

        with self.lock:
            return [
                json.loads(row[0])
                for row in self.connection.execute(
                    "SELECT body FROM outputs WHERE execution_id=? AND ordinal>=? ORDER BY ordinal LIMIT 500",
                    (execution_id, offset),
                )
            ]

    def close(self) -> None:
        """幂等关闭数据库；迟到的观察回调不再写入。"""

        with self.lock:
            if not self._closed:
                self._closed = True
                self.connection.close()
