"""按实际节点触发采集参数、原始输出与标准日志。"""

from __future__ import annotations

import dataclasses
import json
import logging
import re
import traceback
from collections.abc import Iterator, Mapping
from contextlib import suppress
from datetime import datetime, timezone
from threading import RLock
from typing import Any

from pydantic_core import to_jsonable_python

from interlace import Runtime
from interlace.engine.hooks import HookPhase, NodeCall, NodeHook
from interlace.engine.observation import RuntimeEvent, current_node_event

from .store import ServiceStore

# 字段名包含任一片段即视为凭据，按不区分大小写的子串匹配；auth 排除 author，sig 只匹配独立片段。
_SECRET_KEY = re.compile(
    r"secret|token|password|passwd|pwd|passphrase|key|authorization|auth(?!or)"
    r"|cookie|credential|session|signature|(?:^|[_.-])sig(?:$|[_.-])",
    re.IGNORECASE,
)


def _redact_assignment(match: re.Match[str]) -> str:
    """保留凭据名、分隔符与值两侧的引号，只替换值本身。"""

    value = match.group(3)
    quote = value[0] if value[0] in "\"'" else ""
    return f"{match.group(1)}{match.group(2)}{quote}[REDACTED]{quote}"


# 自由文本中的凭据形态：认证头整行值、URL userinfo、Bearer/Basic 凭据、签名查询参数与
# “名称=值”或“名称: 值”。自由文本不使用单独的 key 片段，避免把 KeyError 等异常名称当成凭据。
_TEXT_PATTERNS: tuple[tuple[re.Pattern[str], Any], ...] = (
    (
        # 认证头可能使用 Token、Digest 等任意方案，值一直延续到行尾。
        re.compile(
            r"\b((?:proxy-)?authorization[\"']?\s*[=:]\s*)[^\r\n\"',;]+", re.IGNORECASE
        ),
        r"\1[REDACTED]",
    ),
    (
        # userinfo 可能含 / 或 @，匹配到查询与片段之前的最后一个 @。
        re.compile(r"\b([a-z][a-z0-9+.-]*://)[^\s?#]*@", re.IGNORECASE),
        r"\1[REDACTED]@",
    ),
    (
        re.compile(r"\b(bearer|basic)\s+[\w.~+/=-]+", re.IGNORECASE),
        r"\1 [REDACTED]",
    ),
    (
        # 预签名 URL 的签名与临时凭据参数本身即可授权访问。
        re.compile(
            r"([?&](?:sig|x-amz-signature|x-amz-credential|x-amz-security-token"
            r"|x-goog-signature|x-goog-credential)=)[^\s&#'\"]+",
            re.IGNORECASE,
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            r"([\w.-]*(?:secret|token|password|passwd|pwd|passphrase|api[_-]?key"
            r"|access[_-]?key|private[_-]?key|authorization|cookie|credential"
            r"|signature|session[_-]?id)[\w.-]*)"
            r"([\"']?\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&'\"]+)",
            re.IGNORECASE,
        ),
        _redact_assignment,
    ),
)


def protect_iterators(value: Any, depth: int = 0) -> Any:
    """返回把一次性迭代器替换为不可用标记的副本，避免序列化耗尽并改变原对象。

    递归处理映射、序列、集合与数据类字段；其他对象原样返回，由序列化器按自身规则处理。

    Args:
        value: 待序列化的参数、配置或 Output。
        depth: 当前递归层数，调用方通常省略；超过 16 层不再展开。

    Returns:
        不包含一次性迭代器的结构；容器为新建副本，叶子对象沿用原引用。
    """

    if isinstance(value, Iterator):
        return {"$unavailable": f"{type(value).__name__} 是一次性迭代器，不读取"}
    if depth > 16:
        return value
    if isinstance(value, Mapping):
        return {key: protect_iterators(item, depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [protect_iterators(item, depth + 1) for item in value]
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: protect_iterators(getattr(value, field.name), depth + 1)
            for field in dataclasses.fields(value)
        }
    return value


def contains_iterator(value: Any, depth: int = 0) -> bool:
    """判断值的映射、序列、集合或数据类字段中是否含有一次性迭代器。

    Args:
        value: 待编码的值。
        depth: 当前递归层数，调用方通常省略；超过 16 层不再展开。

    Returns:
        含有迭代器时为 True。
    """

    if isinstance(value, Iterator):
        return True
    if depth > 16:
        return False
    if isinstance(value, Mapping):
        return any(contains_iterator(item, depth + 1) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(contains_iterator(item, depth + 1) for item in value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return any(
            contains_iterator(getattr(value, field.name), depth + 1)
            for field in dataclasses.fields(value)
        )
    return False


def redact_text(text: str) -> str:
    """隐藏自由文本中的 URL userinfo、认证头凭据和凭据名对应的值。

    Args:
        text: 日志消息、异常消息、堆栈或其他字符串值。

    Returns:
        凭据部分替换为 `[REDACTED]` 的文本。
    """

    for pattern, replacement in _TEXT_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def snapshot_value(value: Any) -> Any:
    """立即创建可存储的独立参数快照，限制单条记录大小并隐藏凭据。

    Args:
        value: 当前调用边界的参数、配置或 Output。

    Returns:
        JSON 数据；敏感字段与字符串中的凭据被替换，不支持的对象或超过 65,536 个字符的内容
        返回明确的诊断标记。
    """

    try:
        normalized = to_jsonable_python(
            protect_iterators(value),
            fallback=lambda item: {
                "$unavailable": f"{type(item).__name__} 无法编码为 JSON"
            },
        )
        protected = redact_value(normalized)
        text = json.dumps(protected, ensure_ascii=False, allow_nan=False)
        if len(text) > 65536:
            return {"$truncated": True, "preview": text[:65536]}
        return json.loads(text)
    except Exception:
        return {"$unavailable": f"{type(value).__name__} 无法编码为 JSON"}


def redact_value(value: Any, depth: int = 0) -> Any:
    """逐层隐藏敏感键的值和字符串中的凭据，超过 16 层返回截断标记。

    Args:
        value: 已转换为 JSON 基础类型的数据。
        depth: 当前递归层数，调用方通常省略。

    Returns:
        不与输入共享可变容器的脱敏结构。
    """

    if depth > 16:
        return {"$truncated": True}
    if isinstance(value, dict):
        return {
            key: "[REDACTED]"
            if isinstance(key, str) and _SECRET_KEY.search(key)
            else redact_value(item, depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_value(item, depth + 1) for item in value]
    if isinstance(value, str):
        return redact_text(value)
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
                    "message": redact_text(str(value))[:65536],
                    "traceback": redact_text(
                        "".join(
                            traceback.format_exception(
                                type(value), value, value.__traceback__
                            )
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
        # 日志处理器不能通过再次写日志报告存储异常，避免递归。
        with suppress(Exception):
            self.store.append_events(entries)

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
        """提取脱敏后的日志文字及异常堆栈，不拦截进程级 stdout。

        挂在根 logger 上会让标准库认为已有处理器而跳过 logging.lastResort；
        没有其他处理器会处理该记录时，本处理器代为交给 lastResort，保持应用原有输出。

        Args:
            record: 已经通过应用日志级别过滤的记录。
        """

        self._fallback(record)
        event = current_node_event()
        if event is None:
            return
        with suppress(Exception):
            self.diagnostics.record(
                event,
                "node.log",
                {
                    "level": record.levelname,
                    "logger": record.name,
                    "message": redact_text(record.getMessage())[:65536],
                    "traceback": redact_text(
                        logging.Formatter().formatException(record.exc_info)
                    )[:65536]
                    if record.exc_info
                    else None,
                },
            )

    def _fallback(self, record: logging.LogRecord) -> None:
        """应用未配置其他处理器时，把达到级别的记录交给 logging.lastResort。

        Args:
            record: 当前日志记录。
        """

        last_resort = logging.lastResort
        if last_resort is None or record.levelno < last_resort.level:
            return
        logger: logging.Logger | None = logging.getLogger(record.name)
        while logger is not None:
            if any(handler is not self for handler in logger.handlers):
                return
            if not logger.propagate:
                break
            logger = logger.parent
        last_resort.handle(record)
