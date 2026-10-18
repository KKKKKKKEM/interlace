"""服务层 SQLite 文档和观测历史；不承担任务恢复或消息交付。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterable
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

from .errors import ConflictError, InvalidRequestError, NotFoundError

# 当前代码能够读写的最高数据库结构版本，对应 SQLite PRAGMA user_version。
SCHEMA_VERSION = 3
_NOW = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
_PROJECT_FIELDS = (
    "id",
    "name",
    "description",
    "created_at",
    "updated_at",
)  # 项目行列序。


def registered_name(name: str, version: int) -> str:
    """返回发布版本在 Runtime 中的注册名，例如 `greeting@v2`。"""

    return f"{name}@v{version}"


def _dump(value: Any) -> str:
    """按严格 JSON 序列化数据库正文，保留非 ASCII 字符。"""

    return json.dumps(value, ensure_ascii=False, allow_nan=False)


class ServiceStore:
    """将项目、草稿、发布版本、执行快照、观测和终端输出保存到本地 SQLite。

    Attributes:
        lock: 串行化当前连接上的全部读写事务。
        connection: 当前实例独占的 SQLite 连接，跨线程使用时必须持有 lock。
        retention: 最多保留的执行记录数，None 表示不清理；超出时删除最早创建的已结束执行
            及其观测、诊断与输出。
        _closed: 连接是否已关闭；关闭后迟到的写入被忽略。
    """

    def __init__(
        self, path: str | Path = ":memory:", *, retention: int | None = 1000
    ) -> None:
        """打开数据库、建立服务表并按版本迁移旧数据。

        Args:
            path: 数据库文件，默认只在内存中保留。
            retention: 最多保留的执行记录数，None 表示不清理。

        Raises:
            ValueError: retention 不是正整数或 None。
            RuntimeError: 数据库由更新版本的服务创建，当前代码无法安全读写。
        """

        if retention is not None and (type(retention) is not int or retention < 1):
            raise ValueError("retention must be a positive integer or None")
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = RLock()
        self.retention = retention
        self._closed = False
        self.connection = sqlite3.connect(str(path), check_same_thread=False)
        try:
            self._migrate()
        except BaseException:
            self.connection.close()
            raise

    def _migrate(self) -> None:
        """建立基础表并逐级执行结构迁移，拒绝未知的更高版本。"""

        version = self.connection.execute("PRAGMA user_version").fetchone()[0]
        if version > SCHEMA_VERSION:
            raise RuntimeError(
                f"service database schema {version} is newer than supported "
                f"{SCHEMA_VERSION}"
            )
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            f"""
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
                created_at TEXT NOT NULL DEFAULT ({_NOW}),
                updated_at TEXT NOT NULL DEFAULT ({_NOW})
            );
            CREATE TABLE IF NOT EXISTS definition_projects (
                name TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(id),
                updated_at TEXT NOT NULL DEFAULT ({_NOW})
            );
            CREATE INDEX IF NOT EXISTS project_definitions ON definition_projects(project_id);
            """
        )
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO projects(id, name, description) "
                "VALUES ('default', '默认项目', '')"
            )
            if version < 1:
                # 将已有草稿与发布版本一次性归入默认项目，保持原运行注册名与历史数据。
                self.connection.execute(
                    "INSERT OR IGNORE INTO definition_projects(name, project_id) "
                    "SELECT name, 'default' FROM documents "
                    "UNION SELECT name, 'default' FROM versions"
                )
            if version < 2:
                self.connection.execute("ALTER TABLE executions ADD COLUMN graph TEXT")
            if version < 3:
                self.connection.execute(
                    "ALTER TABLE executions ADD COLUMN definition TEXT"
                )
                self._index_executions(adopt=version < 2)
            self.connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def _index_executions(self, *, adopt: bool) -> None:
        """为全部历史执行回填 graph 与逻辑定义列，供范围查询先筛选再限量。

        Args:
            adopt: 是否把历史执行引用的定义归入默认项目；只有尚未建立项目归属的旧库需要。
        """

        names = {
            registered_name(name, version): name
            for name, version in self.connection.execute(
                "SELECT name, version FROM versions"
            ).fetchall()
        }
        # 先物化全部行再更新同一张表，避免边遍历边修改游标。
        rows = self.connection.execute("SELECT id, body FROM executions").fetchall()
        for execution_id, body in rows:
            graph = json.loads(body).get("graph")
            if not isinstance(graph, str):
                continue
            definition = names.get(graph, graph)
            self.connection.execute(
                "UPDATE executions SET graph=?, definition=? WHERE id=?",
                (graph, definition, execution_id),
            )
            if adopt:
                self.connection.execute(
                    "INSERT OR IGNORE INTO definition_projects(name, project_id) "
                    "VALUES (?, 'default')",
                    (definition,),
                )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS execution_definition ON executions(definition)"
        )

    def projects(self) -> list[dict[str, Any]]:
        """读取持久项目清单。

        Returns:
            按创建时间排列的项目标识、名称、描述和创建更新时间。
        """

        with self.lock:
            return [
                dict(zip(_PROJECT_FIELDS, row, strict=True))
                for row in self.connection.execute(
                    "SELECT id, name, description, created_at, updated_at "
                    "FROM projects ORDER BY created_at, id"
                )
            ]

    def project(self, project_id: str) -> dict[str, Any]:
        """按标识读取项目。

        Args:
            project_id: 项目稳定标识。

        Returns:
            项目元数据。

        Raises:
            NotFoundError: 项目不存在。
        """

        with self.lock:
            row = self.connection.execute(
                "SELECT id, name, description, created_at, updated_at "
                "FROM projects WHERE id=?",
                (project_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"项目不存在：{project_id}")
        return dict(zip(_PROJECT_FIELDS, row, strict=True))

    def save_project(
        self, name: str, description: str, project_id: str | None = None
    ) -> dict[str, Any]:
        """创建项目或修改显示信息，保留稳定标识。

        Args:
            name: 项目显示名称，首尾空白会被去除。
            description: 项目描述。
            project_id: 已有项目标识，None 创建新项目。

        Returns:
            保存后的项目元数据。

        Raises:
            InvalidRequestError: 名称为空。
            ConflictError: 名称已被其他项目使用。
            NotFoundError: 更新目标不存在。
        """

        name = name.strip()
        if not name:
            raise InvalidRequestError("项目名称不能为空")
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
                    f"UPDATE projects SET name=?, description=?, updated_at={_NOW} "
                    "WHERE id=?",
                    (name, description, project_id),
                )
            return self.project(project_id)

    def delete_project(self, project_id: str) -> None:
        """删除不含定义的非默认项目。

        Args:
            project_id: 待删除项目。

        Raises:
            ConflictError: 项目仍有定义或属于默认项目。
            NotFoundError: 项目不存在。
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
        """读取已持久化的定义项目归属。

        Returns:
            按定义全局标识索引的项目标识与归属更新时间。
        """

        with self.lock:
            return {
                name: {"project_id": project_id, "updated_at": updated_at}
                for name, project_id, updated_at in self.connection.execute(
                    "SELECT name, project_id, updated_at FROM definition_projects"
                )
            }

    def adopt_definitions(self, names: Iterable[str]) -> None:
        """将尚无归属的定义归入默认项目，已有归属保持不变。

        Args:
            names: 代码图名称或历史执行引用的逻辑定义标识。
        """

        with self.lock:
            if self._closed:
                return
            with self.connection:
                self.connection.executemany(
                    "INSERT OR IGNORE INTO definition_projects(name, project_id) "
                    "VALUES (?, 'default')",
                    [(name,) for name in names],
                )

    def assign_definition(self, name: str, project_id: str) -> None:
        """设置定义的管理归属，不修改版本内容或运行注册名称。

        调用方负责确认定义存在；尚未持久化归属的代码图在此建立归属。

        Args:
            name: 已知定义标识。
            project_id: 目标项目标识。

        Raises:
            NotFoundError: 目标项目不存在。
        """

        with self.lock, self.connection:
            self.project(project_id)
            self.connection.execute(
                "INSERT INTO definition_projects(name, project_id) VALUES (?, ?) "
                "ON CONFLICT(name) DO UPDATE SET project_id=excluded.project_id, "
                f"updated_at={_NOW}",
                (name, project_id),
            )

    def require_definition_project(self, name: str, project_id: str) -> None:
        """校验保存目标项目存在，且与定义已有归属一致。

        Args:
            name: 定义全局标识。
            project_id: 本次保存指定的项目。

        Raises:
            ConflictError: 标识已属于其他项目。
            NotFoundError: 项目不存在。
        """

        with self.lock:
            self.project(project_id)
            row = self.connection.execute(
                "SELECT project_id FROM definition_projects WHERE name=?", (name,)
            ).fetchone()
        if row is not None and row[0] != project_id:
            raise ConflictError("定义标识已属于其他项目，请使用其他标识或先移动定义")

    def drafts(self) -> list[dict[str, Any]]:
        """读取全部草稿修订快照。

        Returns:
            按名称排列的草稿文档、修订号和所属项目。
        """

        with self.lock:
            return [
                {
                    "revision": revision,
                    "definition": json.loads(body),
                    "project_id": project_id,
                }
                for revision, body, project_id in self.connection.execute(
                    "SELECT d.revision, d.body, p.project_id FROM documents d "
                    "JOIN definition_projects p ON p.name=d.name ORDER BY d.name"
                )
            ]

    def draft(self, name: str) -> dict[str, Any] | None:
        """读取单个草稿。

        Args:
            name: 定义全局标识。

        Returns:
            草稿文档、修订号和所属项目；没有草稿时为 None。
        """

        with self.lock:
            row = self.connection.execute(
                "SELECT d.revision, d.body, p.project_id FROM documents d "
                "JOIN definition_projects p ON p.name=d.name WHERE d.name=?",
                (name,),
            ).fetchone()
        if row is None:
            return None
        return {
            "revision": row[0],
            "definition": json.loads(row[1]),
            "project_id": row[2],
        }

    def _claim_document(self, name: str, revision: int, project_id: str) -> None:
        """在当前事务内校验草稿写入：项目一致、标识未被其他定义占用、修订号未过期。

        Raises:
            NotFoundError: 项目不存在。
            ConflictError: 标识属于其他项目、已被代码图或历史定义占用，或修订号已过期。
        """

        self.require_definition_project(name, project_id)
        current = self.connection.execute(
            "SELECT revision FROM documents WHERE name=?", (name,)
        ).fetchone()
        if (
            current is None
            and self.connection.execute(
                "SELECT 1 FROM definition_projects WHERE name=?", (name,)
            ).fetchone()
        ):
            raise ConflictError("定义标识已存在，请使用其他标识")
        if (0 if current is None else current[0]) != revision:
            raise ConflictError("草稿已更新，请重新打开后合并修改")

    def _write_document(
        self, name: str, encoded: str, revision: int, project_id: str
    ) -> None:
        """在当前事务内写入新修订号的草稿并刷新归属时间。"""

        self.connection.execute(
            "INSERT OR REPLACE INTO documents VALUES (?, ?, ?)",
            (name, revision + 1, encoded),
        )
        self.connection.execute(
            "INSERT INTO definition_projects(name, project_id) VALUES (?, ?) "
            f"ON CONFLICT(name) DO UPDATE SET updated_at={_NOW}",
            (name, project_id),
        )

    def save_draft(
        self,
        name: str,
        body: dict[str, Any],
        revision: int,
        project_id: str = "default",
    ) -> int:
        """比较修订号后原子保存草稿；新标识不得与任何已有定义重复。

        Args:
            name: 定义全局标识。
            body: 完整 JSON 文档。
            revision: 调用方基于的修订号，新建草稿为零。
            project_id: 所属项目，默认进入默认项目。

        Returns:
            新修订号。

        Raises:
            NotFoundError: 项目不存在。
            ConflictError: 修订号已过期、标识属于其他项目，或已被代码图等其他定义占用。
        """

        encoded = _dump(body)
        with self.lock, self.connection:
            self._claim_document(name, revision, project_id)
            self._write_document(name, encoded, revision, project_id)
        return revision + 1

    def publish(
        self,
        name: str,
        body: dict[str, Any],
        revision: int,
        project_id: str,
        register: Callable[[int], object],
    ) -> int:
        """在同一事务中保存新版本与草稿，并在提交前调用注册回调。

        Args:
            name: 定义全局标识。
            body: 已通过冻结校验的完整文档。
            revision: 调用方基于的草稿修订号。
            project_id: 所属项目。
            register: 接收新版本号的回调，通常向 Runtime 注册；抛出异常时整个事务回滚。

        Returns:
            新发布的版本号，从 1 开始递增。

        Raises:
            NotFoundError: 项目不存在。
            ConflictError: 修订号已过期、标识属于其他项目或被其他定义占用。
        """

        encoded = _dump(body)
        with self.lock, self.connection:
            self._claim_document(name, revision, project_id)
            current = self.connection.execute(
                "SELECT MAX(version) FROM versions WHERE name=?", (name,)
            ).fetchone()[0]
            version = (current or 0) + 1
            self.connection.execute(
                "INSERT INTO versions VALUES (?, ?, ?)", (name, version, encoded)
            )
            self._write_document(name, encoded, revision, project_id)
            register(version)
        return version

    def versions(self) -> list[dict[str, Any]]:
        """读取不可变发布版本。

        Returns:
            按名称和版本号排列的名称、版本号及原始图文档。
        """

        with self.lock:
            return [
                {"name": name, "version": version, "definition": json.loads(body)}
                for name, version, body in self.connection.execute(
                    "SELECT name, version, body FROM versions ORDER BY name, version"
                )
            ]

    def record_execution(self, snapshot: dict[str, Any]) -> None:
        """保存一个执行的最新快照；已结束的记录不会被未结束快照覆盖。

        新执行写入后按 retention 清理最早创建的已结束执行。

        Args:
            snapshot: 含 id 与 done 的执行快照；graph 与 definition_name 用于范围查询。
        """

        with self.lock:
            if self._closed:
                return
            with self.connection:
                current = self.connection.execute(
                    "SELECT body FROM executions WHERE id=?", (snapshot["id"],)
                ).fetchone()
                if current is None:
                    self.connection.execute(
                        "INSERT INTO executions(id, body, graph, definition) "
                        "VALUES (?, ?, ?, ?)",
                        (
                            snapshot["id"],
                            _dump(snapshot),
                            snapshot.get("graph"),
                            snapshot.get("definition_name"),
                        ),
                    )
                    self._prune()
                elif not json.loads(current[0])["done"] or snapshot["done"]:
                    self.connection.execute(
                        "UPDATE executions SET body=?, graph=?, definition=? WHERE id=?",
                        (
                            _dump(snapshot),
                            snapshot.get("graph"),
                            snapshot.get("definition_name"),
                            snapshot["id"],
                        ),
                    )

    def _prune(self) -> None:
        """在当前事务内删除超出 retention 的最早已结束执行及其观测与输出。"""

        if self.retention is None:
            return
        rows = self.connection.execute(
            "SELECT id, body FROM executions ORDER BY rowid DESC LIMIT -1 OFFSET ?",
            (self.retention,),
        ).fetchall()
        expired = [(item,) for item, body in rows if json.loads(body).get("done")]
        if expired:
            self.connection.executemany(
                "DELETE FROM events WHERE execution_id=?", expired
            )
            self.connection.executemany(
                "DELETE FROM outputs WHERE execution_id=?", expired
            )
            self.connection.executemany("DELETE FROM executions WHERE id=?", expired)

    def interrupt_unfinished(self, message: str) -> None:
        """把上一进程遗留的未结束记录标记为 interrupted，不恢复任务。

        Args:
            message: 写入快照 error 字段的说明。
        """

        with self.lock, self.connection:
            rows = self.connection.execute("SELECT id, body FROM executions").fetchall()
            for execution_id, body in rows:
                snapshot = json.loads(body)
                if snapshot.get("done"):
                    continue
                snapshot.update(status="interrupted", done=True, error=message)
                self.connection.execute(
                    "UPDATE executions SET body=? WHERE id=?",
                    (_dump(snapshot), execution_id),
                )

    def executions(
        self, *, definition_name: str | None = None, project_id: str | None = None
    ) -> list[dict[str, Any]]:
        """读取最近创建的五百条历史执行快照。

        Args:
            definition_name: 可选逻辑定义范围，包含其全部发布版本。
            project_id: 可选项目范围，在应用数量上限之前筛选。

        Returns:
            按创建顺序倒序排列的快照，definition_name 取自索引列。
        """

        conditions = []
        parameters: list[str] = []
        if definition_name is not None:
            conditions.append("definition=?")
            parameters.append(definition_name)
        if project_id is not None:
            conditions.append(
                "definition IN (SELECT name FROM definition_projects WHERE project_id=?)"
            )
            parameters.append(project_id)
        query = "SELECT body, definition FROM executions"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        with self.lock:
            rows = self.connection.execute(
                query + " ORDER BY rowid DESC LIMIT 500", parameters
            ).fetchall()
        return [{**json.loads(body), "definition_name": name} for body, name in rows]

    def run_summary(self) -> dict[str, tuple[int, dict[str, Any]]]:
        """按逻辑定义汇总历史执行数量与最近创建的一次执行。

        Returns:
            定义标识到 (执行数量, 最近执行快照) 的映射。
        """

        with self.lock:
            rows = self.connection.execute(
                "SELECT e.definition, s.count, e.body FROM executions e JOIN ("
                "SELECT definition, COUNT(*) AS count, MAX(rowid) AS last "
                "FROM executions WHERE definition IS NOT NULL GROUP BY definition"
                ") s ON e.rowid = s.last"
            ).fetchall()
        return {
            name: (count, {**json.loads(body), "definition_name": name})
            for name, count, body in rows
        }

    def recorded(self, execution_ids: Iterable[str]) -> set[str]:
        """返回给定执行中已有历史记录的标识。

        Args:
            execution_ids: 待检查的执行标识。

        Returns:
            已保存快照的执行标识集合。
        """

        found: set[str] = set()
        ids = list(execution_ids)
        with self.lock:
            for start in range(0, len(ids), 500):
                chunk = ids[start : start + 500]
                found.update(
                    row[0]
                    for row in self.connection.execute(
                        "SELECT id FROM executions WHERE id IN "
                        f"({', '.join('?' * len(chunk))})",
                        chunk,
                    )
                )
        return found

    def execution(self, execution_id: str) -> dict[str, Any]:
        """按标识读取历史快照。

        Args:
            execution_id: 执行标识。

        Returns:
            已保存的快照。

        Raises:
            NotFoundError: 没有此记录。
        """

        with self.lock:
            row = self.connection.execute(
                "SELECT body, definition FROM executions WHERE id=?", (execution_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"执行不存在：{execution_id}")
        return {**json.loads(row[0]), "definition_name": row[1]}

    def append_event(self, execution_id: str, body: dict[str, Any]) -> None:
        """追加一条只读观测记录。

        Args:
            execution_id: 关联执行。
            body: 不携带业务值的生命周期元数据。
        """

        self.append_events(((execution_id, body),))

    def append_events(
        self, entries: tuple[tuple[str | None, dict[str, Any]], ...]
    ) -> None:
        """在一次事务中追加一批观测记录。

        Args:
            entries: 按观测顺序排列的 execution ID 与事件内容。
        """

        if not entries:
            return
        rows = tuple((execution_id, _dump(body)) for execution_id, body in entries)
        with self.lock:
            if self._closed:
                return
            with self.connection:
                self.connection.executemany(
                    "INSERT INTO events(execution_id, body) VALUES (?, ?)", rows
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
                    "SELECT id, body FROM events WHERE execution_id=? AND id>? "
                    "ORDER BY id LIMIT 500",
                    (execution_id, after),
                )
            ]

    def append_output(self, execution_id: str, ordinal: int, output: Any) -> None:
        """按稳定序号保存已结束执行的可编码终端输出，重复序号被忽略。

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
                    (execution_id, ordinal, _dump(output)),
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
                    "SELECT body FROM outputs WHERE execution_id=? AND ordinal>=? "
                    "ORDER BY ordinal LIMIT 500",
                    (execution_id, offset),
                )
            ]

    def close(self) -> None:
        """幂等关闭数据库；迟到的观察回调不再写入。"""

        with self.lock:
            if not self._closed:
                self._closed = True
                self.connection.close()
