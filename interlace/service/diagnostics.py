"""按实际节点触发采集参数、原始输出与标准日志。"""

from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timezone
from threading import RLock
from typing import Any

from pydantic_core import to_jsonable_python

from interlace import Runtime
from interlace.engine.hooks import HookPhase, NodeCall, NodeHook
from interlace.engine.observation import RuntimeEvent, current_node_event

from .store import ServiceStore

# 按字段名隐藏常见凭据；自由文本日志由应用自身控制。
_SECRET_FIELDS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "access_token",
        "refresh_token",
        "authorization",
        "api_key",
        "apikey",
        "cookie",
        "set-cookie",
    }
)


def snapshot_value(value: Any) -> Any:
    """立即创建可存储的独立参数快照，限制单条记录大小并隐藏凭据字段。

    Args:
        value: 当前调用边界的参数、配置或 Output。

    Returns:
        JSON 数据；不支持的对象或过大内容返回明确的诊断标记。
    """

    try:
        normalized = to_jsonable_python(
            value,
            fallback=lambda item: {
                "$unavailable": f"{type(item).__name__} 无法编码为 JSON"
            },
        )
        protected = _redact(normalized)
        text = json.dumps(protected, ensure_ascii=False, allow_nan=False)
        if len(text) > 65536:
            return {"$truncated": True, "preview": text[:65536]}
        return json.loads(text)
    except Exception:
        return {"$unavailable": f"{type(value).__name__} 无法编码为 JSON"}


def _redact(value: Any, depth: int = 0) -> Any:
    """逐层隐藏敏感键并限制递归深度。

    Args:
        value: 已转换为 JSON 基础类型的数据。
        depth: 当前递归层数。

    Returns:
        独立的脱敏结构。
    """

    if depth > 16:
        return {"$truncated": True}
    if isinstance(value, dict):
        return {
            key: "[REDACTED]"
            if key.lower() in _SECRET_FIELDS
            else _redact(item, depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item, depth + 1) for item in value]
    return value


class NodeDiagnostics(NodeHook):
    """共享 Hook 贡献通道的节点诊断采集器，不转换输入输出。

    Attributes:
        store: 调用方管理的服务历史存储。
        _active: 当前正在运行的执行标识、节点和 firing 序号。
        _lock: 保护并发 firing 的采集范围。
        _hook: 本次诊断独立拥有的 Hook 注册。
        _handler: 本次诊断独立拥有的标准日志处理器。
        _pending: 按 firing 暂存、等待批量写入的诊断事件。
        _batch_size: 单次事务最多积累的诊断事件数量。
        _closed: 是否已停止采集。
    """

    def __init__(self, runtime: Runtime, store: ServiceStore) -> None:
        """为指定 Runtime 安装诊断 Hook 和带作用域的日志处理器。

        Args:
            runtime: 提供 Node Hook 扩展的运行时。
            store: 保存采集结果的服务存储。
        """

        self.store = store
        self._active: set[tuple[str | None, str | None, Any]] = set()
        self._pending: dict[
            tuple[str | None, str | None, Any],
            list[tuple[str | None, dict[str, Any]]],
        ] = {}
        self._batch_size = 64
        self._lock = RLock()
        self._closed = False
        self._handler = _NodeLogHandler(self)
        self._hook = runtime.attach(self)
        try:
            logging.getLogger().addHandler(self._handler)
        except BaseException:
            self._hook.detach()
            self._handler.close()
            raise

    def observe(self, event: RuntimeEvent) -> None:
        """按生命周期限定日志归属，排除节点结束后的后台任务日志。

        Args:
            event: 当前 Runtime 的生命周期事件。
        """

        key = (event.execution_id, event.node, event.attributes.get("step"))
        with self._lock:
            if event.kind.value == "node.started" and not self._closed:
                self._active.add(key)
            elif event.kind.value == "node.finished":
                self._active.discard(key)
                pending = tuple(self._pending.pop(key, ()))
                self._flush(pending)

    def inspect(self, call: NodeCall, phase: HookPhase, value: Any) -> None:
        """在所有输入 Hook 后、原始输出变换前采集实际调用数据。

        Args:
            call: 真正传给 Node.execute 的调用。
            phase: 输入、原始输出或异常阶段。
            value: 输出或异常，输入阶段为 None。
        """

        event = current_node_event()
        if event is None:
            return
        if phase is HookPhase.ENTER:
            self.record(
                event,
                "node.input",
                {
                    "inputs": snapshot_value(dict(call.inputs)),
                    "options": snapshot_value(dict(call.context.options)),
                },
            )
        elif phase is HookPhase.EXIT:
            self.record(event, "node.output", {"output": snapshot_value(value)})
        elif phase is HookPhase.ERROR:
            self.record(
                event,
                "node.exception",
                {
                    "error_type": type(value).__name__,
                    "message": str(value)[:65536],
                    "traceback": "".join(
                        traceback.format_exception(
                            type(value), value, value.__traceback__
                        )
                    )[:65536],
                },
            )

    def record(
        self, event: RuntimeEvent, kind: str, attributes: dict[str, Any]
    ) -> None:
        """在活动 firing 内保存采集事件，数据库失败不改变节点行为。

        Args:
            event: 节点触发身份。
            kind: 采集事件类型。
            attributes: 已编码且独立的诊断数据。
        """

        key = (event.execution_id, event.node, event.attributes.get("step"))
        body = {
            "kind": kind,
            "graph": event.graph,
            "node": event.node,
            "execution_id": event.execution_id,
            "status": None,
            "attributes": {
                "step": event.attributes.get("step"),
                **attributes,
            },
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            if self._closed or key not in self._active:
                return
            pending = self._pending.setdefault(key, [])
            pending.append((event.execution_id, body))
            if len(pending) >= self._batch_size:
                batch = tuple(pending)
                pending.clear()
                self._flush(batch)

    def _flush(self, entries: tuple[tuple[str | None, dict[str, Any]], ...]) -> None:
        """批量保存诊断事件，存储失败不进入业务执行结果。

        Args:
            entries: 按采集顺序排列的诊断事件。
        """

        if not entries:
            return
        try:
            self.store.append_events(entries)
        except Exception:
            # 日志处理器不能通过再次写日志报告存储异常，避免递归。
            pass

    def close(self) -> None:
        """注销本次 Hook 和日志处理器，不改变其他日志配置。"""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._active.clear()
            pending = tuple(
                entry for entries in self._pending.values() for entry in entries
            )
            self._pending.clear()
        self._flush(pending)
        self._hook.detach()
        logging.getLogger().removeHandler(self._handler)
        self._handler.close()


class _NodeLogHandler(logging.Handler):
    """仅保存当前 Runtime 活动节点发出的 Python logging 记录。

    Attributes:
        diagnostics: 管理活动 firing 与历史存储的采集器。
    """

    def __init__(self, diagnostics: NodeDiagnostics) -> None:
        """建立不修改根日志级别的处理器。

        Args:
            diagnostics: 当前 Runtime 的节点诊断采集器。
        """

        super().__init__()
        self.diagnostics = diagnostics

    def emit(self, record: logging.LogRecord) -> None:
        """提取日志文字及异常，不拦截进程级 stdout。

        Args:
            record: 已经通过应用日志级别过滤的记录。
        """

        event = current_node_event()
        if event is None:
            return
        try:
            self.diagnostics.record(
                event,
                "node.log",
                {
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage()[:65536],
                    "traceback": logging.Formatter().formatException(record.exc_info)[
                        :65536
                    ]
                    if record.exc_info
                    else None,
                },
            )
        except Exception:
            pass
