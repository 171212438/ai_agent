from __future__ import annotations

import csv
import dataclasses
import errno
import fcntl
import hashlib
import html
import json
import os
import re
import secrets
import sqlite3
import stat
import sys
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence


SCHEMA_VERSION = 1
STATE_MARKER_NAME = ".codex-function-doc-batch-state.json"
STATE_MARKER_APPLICATION = "codex-function-doc-batch"
SUCCESS_STATUSES = {"completed", "success", "succeeded"}
TERMINAL_FAILURE_STATUSES = {
    "failed",
    "failure",
    "error",
    "cancelled",
    "canceled",
    "interrupted",
}
FINAL_PHASES = {"finalanswer", "final"}
AGENT_MESSAGE_TYPES = {"agentmessage"}
TASK_ALLOWED_FIELDS = frozenset(
    {
        "id",
        "enabled",
        "document",
        "target",
        "function",
        "source",
        "prompt",
        "custom_prompt",
    }
)
TASK_REQUIRED_FIELDS = frozenset({"id", "function"})
TASK_CONTENT_FIELDS = frozenset(
    {"document", "target", "prompt", "custom_prompt"}
)
TASK_TEXT_FIELDS = TASK_ALLOWED_FIELDS - {"enabled"}
TASK_ALIAS_PAIRS = (
    ("document", "target"),
    ("prompt", "custom_prompt"),
)
# 旧版本的 STARTING/FAILED/ABANDONED 可能已经越过 Turn RPC，不能复用为
# 安全证据。新版本使用带来源语义的独立状态。
LEGACY_AMBIGUOUS_STATUSES = {"STARTING", "FAILED", "ABANDONED"}
CURRENT_UNRESOLVED_STATUSES = {
    "TURN_NOT_REQUESTED",
    "TURN_START_REQUESTED",
    "RUNNING",
    "INCOMPLETE",
}
UNRESOLVED_STATUSES = LEGACY_AMBIGUOUS_STATUSES | CURRENT_UNRESOLVED_STATUSES
SAFE_FAILED_STATUSES = {"FAILED_BEFORE_REQUEST", "FAILED_TERMINAL"}
STARTABLE_TASK_STATUSES = {"PENDING"} | SAFE_FAILED_STATUSES
RERUNNABLE_TASK_STATUSES = {"SUCCEEDED"} | SAFE_FAILED_STATUSES
NON_BLOCKING_TASK_STATUSES = STARTABLE_TASK_STATUSES | {"DISABLED", "SUCCEEDED"}
TERMINAL_ATTEMPT_STATUSES = SAFE_FAILED_STATUSES | {
    "ABANDONED_BEFORE_REQUEST",
    "SUCCEEDED",
}
KNOWN_TASK_STATUSES = NON_BLOCKING_TASK_STATUSES | UNRESOLVED_STATUSES
KNOWN_ATTEMPT_STATUSES = TERMINAL_ATTEMPT_STATUSES | UNRESOLVED_STATUSES
RUN_LOCK_PROTOCOL = 2
RUN_LOCK_MAX_BYTES = 64 * 1024
RUN_LOCK_MAX_PID = (1 << 31) - 1
RUN_LOCK_MAX_RETRIES = 8
ATOMIC_TEMP_MAX_RETRIES = 32
SQLITE_CONNECTION_TIMEOUT_SECONDS = 5.0
SQLITE_WAL_RETRY_TIMEOUT_SECONDS = SQLITE_CONNECTION_TIMEOUT_SECONDS
SQLITE_WAL_RETRY_INTERVAL_SECONDS = 0.05
SQLITE_BUSY_PRIMARY_CODE = 5
SQLITE_LOCKED_PRIMARY_CODE = 6
SQLITE_MAX_INTEGER = (1 << 63) - 1
SQLITE_PATH_OPEN_MAX_RETRIES = 8


def is_sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    error_code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(error_code, int):
        return error_code & 0xFF in {
            SQLITE_BUSY_PRIMARY_CODE,
            SQLITE_LOCKED_PRIMARY_CODE,
        }

    # Python 3.10 的 sqlite3 异常没有 sqlite_errorcode；只对 SQLite
    # 固定的锁冲突消息做窄范围兼容，不能扩大到其他 OperationalError。
    message = str(exc).strip().casefold()
    lock_messages = (
        "database is locked",
        "database is busy",
        "database table is locked",
        "database schema is locked",
    )
    return any(
        message == candidate or message.startswith(f"{candidate}:")
        for candidate in lock_messages
    )


def _outside_status_condition(
    statuses: Iterable[str],
    alias: str = "",
) -> tuple[str, tuple[str, ...]]:
    prefix = f"{alias}." if alias else ""
    known_statuses = tuple(sorted(statuses))
    placeholders = ", ".join("?" for _ in known_statuses)
    return (
        f"({prefix}status IS NULL OR {prefix}status NOT IN ({placeholders}))",
        known_statuses,
    )


def _unresolved_attempt_condition(alias: str = "") -> tuple[str, tuple[str, ...]]:
    return _outside_status_condition(TERMINAL_ATTEMPT_STATUSES, alias)


def _is_unresolved_task_status(status: Any) -> bool:
    return status not in NON_BLOCKING_TASK_STATUSES


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def require_sqlite_attempt_no(attempt_no: Any) -> int:
    if (
        type(attempt_no) is not int
        or attempt_no < 1
        or attempt_no > SQLITE_MAX_INTEGER
    ):
        raise ValueError("attempt_no 必须是 SQLite 正整数。")
    return attempt_no


def attempt_artifact_basename(task_id: str, attempt_no: int) -> str:
    if not isinstance(task_id, str) or not task_id:
        raise ValueError("工件文件名缺少 task_id。")
    attempt_no = require_sqlite_attempt_no(attempt_no)
    # ordinal 会随任务清单重排，slug 也不是 task_id 的单射；持久工件必须
    # 绑定不可变的完整 Task ID。固定 v2 命名空间同时避免与旧数字前缀格式重名。
    return f"v2-task-{sha256_text(task_id)}-a{attempt_no}"


def parse_bool(value: Any, *, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on", "是"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", "否"}:
        return False
    raise ValueError(f"无法识别布尔值：{value!r}")


def enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else getattr(value, "value", value)


def normalized_token(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(enum_value(value)).lower())


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return jsonable(value.value)
    if isinstance(value, Path):
        return str(value)
    if dataclasses.is_dataclass(value):
        return jsonable(dataclasses.asdict(value))
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return jsonable(model_dump(mode="json", by_alias=True))
        except TypeError:
            return jsonable(model_dump())
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(item) for item in value]
    root = getattr(value, "root", None)
    if root is not None and root is not value:
        return {"root": jsonable(root)}
    if hasattr(value, "__dict__"):
        return {
            str(key): jsonable(item)
            for key, item in vars(value).items()
            if not str(key).startswith("_")
        }
    return repr(value)


def nested_get(value: Any, *keys: str, default: Any = None) -> Any:
    current = value
    for key in keys:
        if current is None:
            return default
        if isinstance(current, Mapping):
            current = current.get(key, default)
        else:
            current = getattr(current, key, default)
    return current


def unwrap_item(item: Any) -> Any:
    if isinstance(item, Mapping) and "root" in item:
        return item["root"]
    return getattr(item, "root", item)


def assistant_message_parts(items: Iterable[Any]) -> tuple[list[str], list[str]]:
    final_answers: list[str] = []
    unknown_phase: list[str] = []
    for wrapper in items:
        item = unwrap_item(wrapper)
        item_type = normalized_token(nested_get(item, "type"))
        if item_type not in AGENT_MESSAGE_TYPES:
            continue
        text = nested_get(item, "text")
        if not isinstance(text, str) or not text.strip():
            continue
        phase = nested_get(item, "phase")
        phase_token = normalized_token(phase)
        if phase_token in FINAL_PHASES:
            final_answers.append(text)
        elif phase is None or phase_token == "":
            unknown_phase.append(text)
    return final_answers, unknown_phase


def final_response_from_items(items: Sequence[Any]) -> str | None:
    final_answers, unknown_phase = assistant_message_parts(items)
    if final_answers:
        return final_answers[-1]
    if unknown_phase:
        return unknown_phase[-1]
    return None


@dataclass(frozen=True, slots=True)
class TaskSpec:
    task_id: str
    ordinal: int
    enabled: bool
    document: str
    function: str
    source: str = ""
    custom_prompt: str = ""

    @property
    def fingerprint(self) -> str:
        payload = {
            "id": self.task_id,
            "document": self.document,
            "function": self.function,
            "source": self.source,
            "custom_prompt": self.custom_prompt,
        }
        return sha256_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )

    @property
    def filename_slug(self) -> str:
        # 仅用于兼容和显示，不能作为持久工件的身份。
        raw = re.sub(r"[^\w.-]+", "-", self.task_id, flags=re.UNICODE).strip("-._")
        return raw[:80] or f"task-{self.ordinal:03d}"


@dataclass(frozen=True, slots=True)
class Settings:
    config_path: Path
    project_dir: Path
    tasks_file: Path
    output_dir: Path
    thread_name: str
    model: str | None
    effort: str | None
    stream_events: bool
    continue_on_error: bool


@dataclass(slots=True)
class TurnOutcome:
    turn_id: str
    status: str
    error: Any
    started_at: int | None
    completed_at: int | None
    duration_ms: int | None
    final_response: str | None
    items: list[Any]
    usage: Any


class TaskRunResult(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED_TERMINAL = "failed_terminal"
    INCOMPLETE_UNKNOWN = "incomplete_unknown"


class ThreadAdoptionConflict(RuntimeError):
    pass


class TurnCheckpointConflict(RuntimeError):
    pass


class TurnIdentityError(RuntimeError):
    pass


def required_turn_id(value: Any, *, source: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TurnIdentityError(f"{source} 缺少有效的非空 Turn ID。")
    if value != value.strip():
        raise TurnIdentityError(f"{source} 的 Turn ID 含有首尾空白。")
    return value


def matching_turn_id(
    value: Any,
    *,
    expected_turn_id: str,
    source: str,
) -> str:
    try:
        actual_turn_id = required_turn_id(value, source=source)
    except TurnIdentityError as exc:
        raise TurnIdentityError(
            f"{source} 无法绑定预期 Turn ID {expected_turn_id!r}：{exc}"
        ) from exc
    if actual_turn_id != expected_turn_id:
        raise TurnIdentityError(
            f"{source} Turn ID 不一致："
            f"期望 {expected_turn_id!r}，得到 {actual_turn_id!r}。"
        )
    return actual_turn_id


def classify_turn_outcome(outcome: TurnOutcome) -> TaskRunResult:
    status = normalized_token(outcome.status)
    has_final = bool(outcome.final_response and outcome.final_response.strip())
    if status in SUCCESS_STATUSES and has_final:
        return TaskRunResult.SUCCEEDED
    if status in SUCCESS_STATUSES:
        outcome.error = outcome.error or "Turn 已完成，但没有 final_response。"
        return TaskRunResult.FAILED_TERMINAL
    if status in TERMINAL_FAILURE_STATUSES:
        return TaskRunResult.FAILED_TERMINAL
    outcome.error = outcome.error or f"无法确认 Turn 终态：{outcome.status!r}"
    return TaskRunResult.INCOMPLETE_UNKNOWN


def resolve_config_path(raw_value: str, *, relative_to: Path) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(raw_value))
    path = Path(expanded)
    if not path.is_absolute():
        path = relative_to / path
    return path.resolve()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def validate_output_location(project_dir: Path, output_dir: Path) -> None:
    """Require the host control directory and Codex workspace to be disjoint."""
    project = project_dir.resolve()
    output = output_dir.resolve()
    if _is_within(output, project) or _is_within(project, output):
        raise ValueError(
            "output_dir 必须与 project_dir 完全分离，不能位于项目内、等于项目，"
            "也不能是项目的父目录："
            f"\nproject_dir：{project}\noutput_dir：{output}"
        )


def load_settings(config_path: Path) -> Settings:
    config_path = config_path.resolve()
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"找不到配置文件：{config_path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"配置文件不是有效 JSON：{config_path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ValueError("config.json 顶层必须是 JSON 对象。")
    project_raw = str(raw.get("project_dir", "")).strip()
    tasks_raw = str(raw.get("tasks_file", "")).strip()
    output_raw = str(raw.get("output_dir", "")).strip()
    if not project_raw:
        raise ValueError("config.json 缺少 project_dir。")
    if not tasks_raw:
        raise ValueError("config.json 缺少 tasks_file。")
    if not output_raw:
        raise ValueError(
            "config.json 缺少 output_dir；请指定一个位于 project_dir 外的专用目录。"
        )

    config_dir = config_path.parent
    project_dir = resolve_config_path(project_raw, relative_to=config_dir)
    tasks_file = resolve_config_path(tasks_raw, relative_to=config_dir)
    output_dir = resolve_config_path(output_raw, relative_to=config_dir)

    if not project_dir.is_dir():
        raise ValueError(f"project_dir 不存在或不是目录：{project_dir}")
    if not tasks_file.is_file():
        raise ValueError(f"tasks_file 不存在：{tasks_file}")
    validate_output_location(project_dir, output_dir)

    model = raw.get("model")
    effort = raw.get("effort")
    return Settings(
        config_path=config_path,
        project_dir=project_dir,
        tasks_file=tasks_file,
        output_dir=output_dir,
        thread_name=str(raw.get("thread_name", "函数文档批处理")).strip()
        or "函数文档批处理",
        model=str(model).strip() if model not in (None, "") else None,
        effort=str(effort).strip() if effort not in (None, "") else None,
        stream_events=parse_bool(raw.get("stream_events"), default=True),
        continue_on_error=parse_bool(raw.get("continue_on_error"), default=False),
    )


def ensure_inside_project(project_dir: Path, raw_path: str, *, field: str) -> str:
    raw_path = raw_path.strip()
    if not raw_path:
        if field == "source":
            return ""
        raise ValueError(f"{field} 不能为空。")
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = project_dir / candidate
    candidate = candidate.resolve()
    try:
        relative = candidate.relative_to(project_dir.resolve())
    except ValueError as exc:
        raise ValueError(f"{field} 必须位于 project_dir 内：{raw_path}") from exc
    return relative.as_posix()


def _task_from_mapping(
    raw: Mapping[str, Any], ordinal: int, project_dir: Path
) -> TaskSpec:
    task_id = str(raw.get("id", "")).strip()
    function = str(raw.get("function", "")).strip()
    document = str(raw.get("document", raw.get("target", ""))).strip()
    source = str(raw.get("source", "")).strip()
    custom_prompt = str(raw.get("prompt", raw.get("custom_prompt", ""))).strip()
    if not task_id:
        raise ValueError(f"第 {ordinal} 个任务缺少 id。")
    if not function:
        raise ValueError(f"任务 {task_id!r} 缺少 function。")
    if not document and not custom_prompt:
        raise ValueError(f"任务 {task_id!r} 至少需要 document 或 prompt。")
    if document:
        document = ensure_inside_project(project_dir, document, field="document")
    if source:
        source = ensure_inside_project(project_dir, source, field="source")
    return TaskSpec(
        task_id=task_id,
        ordinal=ordinal,
        enabled=parse_bool(raw.get("enabled"), default=True),
        document=document,
        function=function,
        source=source,
        custom_prompt=custom_prompt,
    )


def validate_task_csv_header(fieldnames: Sequence[str] | None) -> tuple[str, ...]:
    if not fieldnames:
        raise ValueError("任务 CSV 缺少表头。")
    if any(not fieldname.strip() for fieldname in fieldnames):
        raise ValueError("任务 CSV 表头包含空字段名。")

    seen: set[str] = set()
    duplicates: set[str] = set()
    for fieldname in fieldnames:
        if fieldname in seen:
            duplicates.add(fieldname)
        seen.add(fieldname)
    if duplicates:
        raise ValueError(
            "任务 CSV 表头包含重复字段：" + ", ".join(sorted(duplicates))
        )

    header_fields = set(fieldnames)
    unknown = header_fields - TASK_ALLOWED_FIELDS
    if unknown:
        raise ValueError(
            "任务 CSV 表头包含未知字段：" + ", ".join(sorted(unknown))
        )
    for canonical, alias in TASK_ALIAS_PAIRS:
        if canonical in header_fields and alias in header_fields:
            raise ValueError(
                f"任务 CSV 表头不能同时包含 {canonical} 和 {alias}。"
            )
    missing = TASK_REQUIRED_FIELDS - header_fields
    if missing:
        raise ValueError(
            "任务 CSV 表头缺少必需字段：" + ", ".join(sorted(missing))
        )
    if not TASK_CONTENT_FIELDS.intersection(header_fields):
        raise ValueError(
            "任务 CSV 表头至少需要 document、target、prompt 或 custom_prompt 之一。"
        )
    return tuple(fieldnames)


def task_json_object_without_duplicates(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"任务 JSON 对象包含重复字段：{key}。")
        result[key] = value
    return result


def reject_task_json_constant(value: str) -> Any:
    raise ValueError(f"任务 JSON 包含非标准数值：{value}。")


def validate_task_json_mapping(
    raw: Mapping[str, Any],
    ordinal: int,
) -> dict[str, Any]:
    fields = set(raw)
    unknown = fields - TASK_ALLOWED_FIELDS
    if unknown:
        raise ValueError(
            f"任务 JSON 第 {ordinal} 个任务包含未知字段："
            + ", ".join(sorted(unknown))
        )
    missing = TASK_REQUIRED_FIELDS - fields
    if missing:
        raise ValueError(
            f"任务 JSON 第 {ordinal} 个任务缺少必需字段："
            + ", ".join(sorted(missing))
        )
    for canonical, alias in TASK_ALIAS_PAIRS:
        if canonical in fields and alias in fields:
            raise ValueError(
                f"任务 JSON 第 {ordinal} 个任务不能同时包含 "
                f"{canonical} 和 {alias}。"
            )
    if not TASK_CONTENT_FIELDS.intersection(fields):
        raise ValueError(
            f"任务 JSON 第 {ordinal} 个任务至少需要 "
            "document、target、prompt 或 custom_prompt 之一。"
        )

    for field in sorted(TASK_TEXT_FIELDS.intersection(fields)):
        if not isinstance(raw[field], str):
            raise ValueError(
                f"任务 JSON 第 {ordinal} 个任务字段 {field} 必须是字符串。"
            )
    if "enabled" in raw and not isinstance(raw["enabled"], bool):
        raise ValueError(
            f"任务 JSON 第 {ordinal} 个任务字段 enabled 必须是 JSON 布尔值。"
        )
    return dict(raw)


def load_tasks(tasks_file: Path, project_dir: Path) -> list[TaskSpec]:
    suffix = tasks_file.suffix.lower()
    raw_tasks: list[Mapping[str, Any]]
    if suffix == ".csv":
        try:
            with tasks_file.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle, strict=True)
                fieldnames = validate_task_csv_header(reader.fieldnames)
                raw_tasks = []
                for row in reader:
                    if None in row:
                        raise ValueError(
                            f"任务 CSV 第 {reader.line_num} 行包含多余列。"
                        )
                    missing_values = [
                        fieldname
                        for fieldname in fieldnames
                        if row.get(fieldname) is None
                    ]
                    if missing_values:
                        raise ValueError(
                            f"任务 CSV 第 {reader.line_num} 行缺少列："
                            + ", ".join(missing_values)
                        )
                    raw_tasks.append(row)
        except csv.Error as exc:
            raise ValueError(f"任务 CSV 无效：{tasks_file}: {exc}") from exc
    elif suffix == ".json":
        try:
            payload = json.loads(
                tasks_file.read_text(encoding="utf-8-sig"),
                object_pairs_hook=task_json_object_without_duplicates,
                parse_constant=reject_task_json_constant,
            )
        except json.JSONDecodeError as exc:
            raise ValueError(f"任务 JSON 无效：{tasks_file}: {exc}") from exc
        if isinstance(payload, dict):
            if "tasks" not in payload:
                raise ValueError("任务 JSON 顶层对象缺少 tasks 字段。")
            unknown = set(payload) - {"tasks"}
            if unknown:
                raise ValueError(
                    "任务 JSON 顶层对象包含未知字段："
                    + ", ".join(sorted(unknown))
                )
            payload = payload["tasks"]
        if not isinstance(payload, list):
            raise ValueError("任务 JSON 顶层必须是数组，或包含 tasks 数组。")
        raw_tasks = []
        for ordinal, item in enumerate(payload, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"任务 JSON 第 {ordinal} 个任务必须是对象。")
            raw_tasks.append(validate_task_json_mapping(item, ordinal))
    else:
        raise ValueError("tasks_file 目前支持 .csv 或 .json。")

    tasks = [
        _task_from_mapping(raw, ordinal, project_dir)
        for ordinal, raw in enumerate(raw_tasks, start=1)
    ]
    seen_ids: set[str] = set()
    seen_targets: set[tuple[str, str, str]] = set()
    for task in tasks:
        if task.task_id in seen_ids:
            raise ValueError(f"重复的任务 id：{task.task_id}")
        seen_ids.add(task.task_id)
        identity = (task.source, task.function, task.document)
        if task.enabled and identity in seen_targets:
            raise ValueError(
                f"重复的启用任务：source={task.source!r}, "
                f"function={task.function!r}, document={task.document!r}"
            )
        if task.enabled:
            seen_targets.add(identity)
    return tasks


def build_prompt(task: TaskSpec, attempt_no: int) -> str:
    source_line = (
        f"- 已知源文件：`{task.source}`"
        if task.source
        else "- 源文件：请在当前工程中定位该函数的完整定义、声明和直接调用点"
    )
    document_line = (
        f"- 目标 Markdown：`{task.document}`"
        if task.document
        else "- 目标 Markdown：按本轮用户任务原文确定"
    )
    custom = (
        f"\n\n本轮用户任务原文：\n{task.custom_prompt}"
        if task.custom_prompt
        else ""
    )
    direct_request = (
        f"请按照最新 AGENTS.md，在 `{task.document}` 中增加 "
        f"`{task.function}` 函数说明。"
        if task.document
        else task.custom_prompt
    )
    return f"""
[batch-task-id: {task.task_id}]
[attempt: {attempt_no}]

这是批处理中的一个独立 Turn。只处理下面这一个函数；不得开始任务清单中的其他函数。

本 Turn 开始时，必须使用文件读取工具重新读取当前工程磁盘上对本任务生效的最新
`AGENTS.md` / `AGENTS.override.md`，不得只依赖同一 Thread 前面 Turn 中已经加载的旧版本。
请遵守其中要求的 Skills、源码核对、Manual/PDF 回源、注释密度、文档顺序和验收规则。

本轮任务：
- 函数：`{task.function}`
{source_line}
{document_line}

{direct_request}

开始修改前先检查目标文档中是否已经存在该函数说明：
- 如果不存在，按最新规则创建。
- 如果已经完整存在，只验证并做必要修正，不得重复插入。
- 如果上一次异常中断留下了半成品，请校正并完成它。

仅修改本任务必要的文件。完成源码、声明、调用点、相关手册/Manifest/PDF 和现有文档核对，
并执行 AGENTS.md 要求的验证后，再给出本函数自己的独立最终回复。

最终回复至少包含：
1. 修改文件和位置
2. 本轮覆盖的函数行为、调用关系和事实边界
3. 验证结果
4. 未执行项、限制或风险
{custom}
""".strip()


def applicable_agents_snapshot(project_dir: Path, task: TaskSpec) -> list[dict[str, Any]]:
    target_parent = (project_dir / task.document).parent if task.document else project_dir
    try:
        relative_parent = target_parent.resolve().relative_to(project_dir.resolve())
    except ValueError:
        relative_parent = Path()

    directories = [project_dir]
    current = project_dir
    for part in relative_parent.parts:
        current = current / part
        directories.append(current)

    snapshot: list[dict[str, Any]] = []
    for directory in directories:
        selected: Path | None = None
        override = directory / "AGENTS.override.md"
        regular = directory / "AGENTS.md"
        if override.is_file() and override.stat().st_size:
            selected = override
        elif regular.is_file() and regular.stat().st_size:
            selected = regular
        if selected is None:
            continue
        data = selected.read_bytes()
        snapshot.append(
            {
                "path": selected.relative_to(project_dir).as_posix(),
                "sha256": hashlib.sha256(data).hexdigest(),
                "size": len(data),
                "mtime_ns": selected.stat().st_mtime_ns,
            }
        )
    return snapshot


class Store:
    _SCHEMA_COLUMN_SIGNATURES = {
        "meta": (
            ("key", "TEXT", 0, None, 1),
            ("value", "TEXT", 1, None, 0),
        ),
        "tasks": (
            ("task_id", "TEXT", 0, None, 1),
            ("ordinal", "INTEGER", 1, None, 0),
            ("enabled", "INTEGER", 1, None, 0),
            ("document", "TEXT", 1, None, 0),
            ("function_name", "TEXT", 1, None, 0),
            ("source", "TEXT", 1, None, 0),
            ("custom_prompt", "TEXT", 1, None, 0),
            ("fingerprint", "TEXT", 1, None, 0),
            ("status", "TEXT", 1, None, 0),
            ("latest_attempt", "INTEGER", 1, "0", 0),
            ("created_at", "TEXT", 1, None, 0),
            ("updated_at", "TEXT", 1, None, 0),
        ),
        "attempts": (
            ("task_id", "TEXT", 1, None, 1),
            ("attempt_no", "INTEGER", 1, None, 2),
            ("thread_id", "TEXT", 0, None, 0),
            ("turn_id", "TEXT", 0, None, 0),
            ("status", "TEXT", 1, None, 0),
            ("started_at", "TEXT", 1, None, 0),
            ("completed_at", "TEXT", 0, None, 0),
            ("duration_ms", "INTEGER", 0, None, 0),
            ("prompt", "TEXT", 1, None, 0),
            ("final_response", "TEXT", 0, None, 0),
            ("error_json", "TEXT", 0, None, 0),
            ("usage_json", "TEXT", 0, None, 0),
            ("agents_snapshot_json", "TEXT", 1, None, 0),
            ("prompt_file", "TEXT", 1, None, 0),
            ("event_file", "TEXT", 1, None, 0),
            ("response_file", "TEXT", 1, None, 0),
        ),
    }
    _SCHEMA_FOREIGN_KEYS = {
        "meta": (),
        "tasks": (),
        "attempts": (
            ("tasks", "task_id", "task_id", "NO ACTION", "NO ACTION", "NONE"),
        ),
    }
    _SCHEMA_DDL = (
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            ordinal INTEGER NOT NULL,
            enabled INTEGER NOT NULL,
            document TEXT NOT NULL,
            function_name TEXT NOT NULL,
            source TEXT NOT NULL,
            custom_prompt TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            status TEXT NOT NULL,
            latest_attempt INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS attempts (
            task_id TEXT NOT NULL,
            attempt_no INTEGER NOT NULL,
            thread_id TEXT,
            turn_id TEXT,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            duration_ms INTEGER,
            prompt TEXT NOT NULL,
            final_response TEXT,
            error_json TEXT,
            usage_json TEXT,
            agents_snapshot_json TEXT NOT NULL,
            prompt_file TEXT NOT NULL,
            event_file TEXT NOT NULL,
            response_file TEXT NOT NULL,
            PRIMARY KEY (task_id, attempt_no),
            FOREIGN KEY (task_id) REFERENCES tasks(task_id)
        )
        """,
    )

    @staticmethod
    def _database_identity(file_stat: os.stat_result) -> tuple[int, int]:
        return file_stat.st_dev, file_stat.st_ino

    @classmethod
    def _validate_database_guard(
        cls,
        db_path: Path,
        descriptor: int,
    ) -> os.stat_result:
        descriptor_stat = os.fstat(descriptor)
        if not stat.S_ISREG(descriptor_stat.st_mode):
            raise ValueError(f"SQLite 路径必须是普通文件：{db_path}")
        if descriptor_stat.st_nlink != 1:
            raise ValueError(f"SQLite 主库不能存在额外硬链接：{db_path}")
        expected_uid = os.geteuid() if hasattr(os, "geteuid") else None
        if expected_uid is not None and descriptor_stat.st_uid != expected_uid:
            raise ValueError(f"SQLite 主库必须属于当前用户：{db_path}")

        try:
            path_stat = os.lstat(db_path)
        except FileNotFoundError as exc:
            raise ValueError(f"SQLite 路径在打开期间消失：{db_path}") from exc
        if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISREG(path_stat.st_mode):
            raise ValueError(
                f"SQLite 路径必须是普通文件且不能是符号链接：{db_path}"
            )
        if path_stat.st_nlink != 1:
            raise ValueError(f"SQLite 主库不能存在额外硬链接：{db_path}")
        if expected_uid is not None and path_stat.st_uid != expected_uid:
            raise ValueError(f"SQLite 主库必须属于当前用户：{db_path}")
        if cls._database_identity(path_stat) != cls._database_identity(
            descriptor_stat
        ):
            raise ValueError(f"SQLite 路径在打开期间发生变化：{db_path}")
        return descriptor_stat

    @classmethod
    def _open_database_guard(cls, db_path: Path) -> int:
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        if not nofollow:
            raise RuntimeError("当前平台不支持安全打开 SQLite 主库。")
        common_flags = (
            nofollow
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        unsafe_errnos = {
            errno.ELOOP,
            errno.ENOTDIR,
            errno.EISDIR,
            errno.ENXIO,
        }
        for _attempt in range(SQLITE_PATH_OPEN_MAX_RETRIES):
            descriptor: int | None = None
            try:
                try:
                    descriptor = open_descriptor(
                        db_path,
                        os.O_RDWR | os.O_CREAT | os.O_EXCL | common_flags,
                        0o600,
                    )
                except FileExistsError:
                    try:
                        descriptor = open_descriptor(
                            db_path,
                            os.O_RDONLY | common_flags,
                        )
                    except FileNotFoundError:
                        continue
            except OSError as exc:
                if exc.errno in unsafe_errnos:
                    raise ValueError(
                        "SQLite 路径必须是普通文件且不能是符号链接："
                        f"{db_path}"
                    ) from exc
                raise
            try:
                cls._validate_database_guard(db_path, descriptor)
            except BaseException:
                os.close(descriptor)
                raise
            return descriptor
        raise RuntimeError(f"SQLite 路径在安全打开期间反复变化：{db_path}")

    def _validate_database_identity(self) -> None:
        descriptor = self._database_guard_fd
        if descriptor is None:
            raise RuntimeError("SQLite 主库保护描述符已经关闭。")
        self._validate_database_guard(self.db_path, descriptor)

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db_path = db_path
        self._database_guard_fd: int | None = self._open_database_guard(db_path)
        connection: sqlite3.Connection | None = None
        try:
            self._validate_database_identity()
            os.fchmod(self._database_guard_fd, 0o600)
            self._validate_database_identity()
            connection = sqlite3.connect(
                str(db_path),
                timeout=SQLITE_CONNECTION_TIMEOUT_SECONDS,
            )
            self.conn = connection
            self._validate_database_identity()
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA foreign_keys = ON")
            self._validate_database_identity()
            self._create_schema()
            self._validate_database_identity()
            # 版本和表结构验收成功后才持久切换 journal mode，避免仅因打开
            # 不兼容数据库就修改其 SQLite 状态。
            self._ensure_wal_mode()
            self._validate_database_identity()
        except BaseException:
            if connection is not None:
                try:
                    connection.close()
                except BaseException:
                    pass
            descriptor = self._database_guard_fd
            self._database_guard_fd = None
            if descriptor is not None:
                os.close(descriptor)
            raise

    def close(self) -> None:
        descriptor = self._database_guard_fd
        self._database_guard_fd = None
        validation_error: BaseException | None = None
        if descriptor is not None:
            try:
                self._validate_database_guard(self.db_path, descriptor)
            except BaseException as exc:
                validation_error = exc

        cleanup_error: BaseException | None = None
        try:
            self.conn.close()
        except BaseException as exc:
            cleanup_error = exc
        if descriptor is not None:
            try:
                os.close(descriptor)
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc

        if validation_error is not None:
            if cleanup_error is not None:
                raise validation_error from cleanup_error
            raise validation_error
        if cleanup_error is not None:
            raise cleanup_error

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        self.close()

    @contextmanager
    def _immediate_transaction(self) -> Iterator[None]:
        if self.conn.in_transaction:
            raise RuntimeError("不能在已有 SQLite 事务中启动独立状态变更。")
        self._validate_database_identity()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._validate_database_identity()
            self.conn.commit()
            self._validate_database_identity()
        except BaseException:
            self.conn.rollback()
            raise

    @staticmethod
    def _journal_mode_value(row: Any) -> str | None:
        if row is None:
            return None
        try:
            value = row[0]
        except (IndexError, KeyError, TypeError):
            return None
        return str(value).casefold()

    def _ensure_wal_mode(self) -> None:
        deadline = time.monotonic() + SQLITE_WAL_RETRY_TIMEOUT_SECONDS
        while True:
            self._validate_database_identity()
            retryable_error: sqlite3.OperationalError | None = None
            resulting_mode: str | None = None
            try:
                current_mode = self._journal_mode_value(
                    self.conn.execute("PRAGMA journal_mode").fetchone()
                )
                if current_mode == "wal":
                    self._validate_database_identity()
                    return
                resulting_mode = self._journal_mode_value(
                    self.conn.execute("PRAGMA journal_mode = WAL").fetchone()
                )
                if resulting_mode == "wal":
                    self._validate_database_identity()
                    return
            except sqlite3.OperationalError as exc:
                if not is_sqlite_lock_error(exc):
                    raise
                retryable_error = exc

            now = time.monotonic()
            if now >= deadline:
                if retryable_error is not None:
                    raise retryable_error
                raise RuntimeError(
                    "SQLite journal_mode 切换为 WAL 失败："
                    f"返回模式={resulting_mode!r}。"
                )
            time.sleep(
                min(
                    SQLITE_WAL_RETRY_INTERVAL_SECONDS,
                    deadline - now,
                )
            )

    @staticmethod
    def _canonicalize_schema_sql(sql: str) -> str:
        characters: list[str] = []
        closing_quote: str | None = None
        index = 0
        while index < len(sql):
            character = sql[index]
            if closing_quote is None:
                if character.isspace():
                    index += 1
                    continue
                if character in {"'", '"', "`", "["}:
                    closing_quote = "]" if character == "[" else character
                    characters.append(character)
                else:
                    characters.append(character.casefold())
            else:
                characters.append(character)
                if character == closing_quote:
                    if closing_quote != "]" and index + 1 < len(sql):
                        if sql[index + 1] == closing_quote:
                            characters.append(sql[index + 1])
                            index += 1
                        else:
                            closing_quote = None
                    else:
                        closing_quote = None
            index += 1

        normalized = "".join(characters).rstrip(";")
        prefix = "createtableifnotexists"
        if normalized.startswith(prefix):
            normalized = "createtable" + normalized[len(prefix) :]
        return normalized

    def _read_schema_version_strict(self) -> str | None:
        object_rows = self.conn.execute(
            """
            SELECT name, type, sql FROM main.sqlite_master
            WHERE name NOT GLOB 'sqlite_*'
            """
        ).fetchall()
        if not object_rows:
            return None

        objects: dict[str, tuple[str, str, str | None]] = {}
        for row in object_rows:
            name = str(row["name"])
            normalized_name = name.casefold()
            if normalized_name in objects:
                raise ValueError(
                    "状态数据库包含名称冲突的 schema 对象；拒绝自动识别。"
                )
            stored_sql = row["sql"]
            objects[normalized_name] = (
                name,
                str(row["type"]),
                stored_sql if isinstance(stored_sql, str) else None,
            )

        expected_tables = set(self._SCHEMA_COLUMN_SIGNATURES)
        unexpected = sorted(
            name
            for normalized_name, (name, _object_type, _sql) in objects.items()
            if normalized_name not in expected_tables
        )
        if unexpected:
            raise ValueError(
                "状态数据库包含当前 schema 不识别的对象："
                f"{', '.join(unexpected)}"
            )
        if "meta" not in objects:
            raise ValueError(
                "状态数据库已有结构但缺少 meta 表和 schema_version；"
                "拒绝自动标记。"
            )

        expected_ddl = dict(zip(self._SCHEMA_COLUMN_SIGNATURES, self._SCHEMA_DDL))
        for table_name, expected_signature in self._SCHEMA_COLUMN_SIGNATURES.items():
            schema_object = objects.get(table_name)
            if schema_object is None:
                continue
            _stored_name, object_type, stored_sql = schema_object
            if object_type != "table":
                raise ValueError(
                    f"状态数据库的 {table_name} 不是表；schema 结构不兼容。"
                )
            if stored_sql is None or self._canonicalize_schema_sql(
                stored_sql
            ) != self._canonicalize_schema_sql(expected_ddl[table_name]):
                raise ValueError(
                    f"状态数据库的 {table_name} 表结构与当前 schema 不兼容。"
                )
            table_info = self.conn.execute(
                f'PRAGMA main.table_info("{table_name}")'
            ).fetchall()
            actual_signature = tuple(
                (
                    str(row["name"]),
                    str(row["type"]).upper(),
                    int(row["notnull"]),
                    row["dflt_value"],
                    int(row["pk"]),
                )
                for row in table_info
            )
            foreign_keys = self.conn.execute(
                f'PRAGMA main.foreign_key_list("{table_name}")'
            ).fetchall()
            actual_foreign_keys = tuple(
                (
                    str(row["table"]),
                    str(row["from"]),
                    str(row["to"]),
                    str(row["on_update"]).upper(),
                    str(row["on_delete"]).upper(),
                    str(row["match"]).upper(),
                )
                for row in sorted(
                    foreign_keys,
                    key=lambda item: (int(item["id"]), int(item["seq"])),
                )
            )
            if (
                actual_signature != expected_signature
                or actual_foreign_keys != self._SCHEMA_FOREIGN_KEYS[table_name]
            ):
                raise ValueError(
                    f"状态数据库的 {table_name} 表结构与当前 schema 不兼容。"
                )

        version_rows = self.conn.execute(
            "SELECT value FROM meta WHERE key = ? LIMIT 2",
            ("schema_version",),
        ).fetchall()
        if len(version_rows) > 1:
            raise ValueError("状态数据库包含重复的 schema_version。")
        if version_rows:
            raw_version = version_rows[0]["value"]
            if not isinstance(raw_version, str) or re.fullmatch(
                r"(?:0|[1-9][0-9]*)",
                raw_version,
                flags=re.ASCII,
            ) is None:
                raise ValueError(
                    "状态数据库 schema_version 无效："
                    f"{raw_version!r}；必须是规范十进制整数。"
                )
            return raw_version

        # 只允许恢复可证明没有任何状态的 partial bootstrap。当前 DDL 的
        # 已知表可以缺失或为空；任何行都意味着版本来源不明。
        for table_name in self._SCHEMA_COLUMN_SIGNATURES:
            if table_name not in objects:
                continue
            if self.conn.execute(
                f'SELECT 1 FROM "{table_name}" LIMIT 1'
            ).fetchone():
                raise ValueError(
                    "状态数据库缺少 schema_version，但已存在状态数据；"
                    "拒绝自动标记。"
                )
        return None

    @staticmethod
    def _require_supported_schema_version(stored_version: str | None) -> None:
        if stored_version is None:
            return
        parsed_version = int(stored_version)
        if parsed_version == SCHEMA_VERSION:
            return
        if parsed_version > SCHEMA_VERSION:
            reason = "数据库来自未来版本，禁止降级打开"
        else:
            reason = "数据库版本较旧，当前程序没有可用迁移"
        raise ValueError(
            "状态数据库 schema_version 不受支持："
            f"现有={stored_version!r}，当前={SCHEMA_VERSION!r}；{reason}。"
        )

    def _create_schema(self) -> None:
        initial_version = self._read_schema_version_strict()
        self._require_supported_schema_version(initial_version)

        with self._immediate_transaction():
            locked_version = self._read_schema_version_strict()
            self._require_supported_schema_version(locked_version)
            if initial_version is not None and locked_version != initial_version:
                raise ValueError(
                    "状态数据库 schema_version 在初始化期间发生变化；"
                    "拒绝继续。"
                )

            for statement in self._SCHEMA_DDL:
                self.conn.execute(statement)

            if locked_version is None:
                self._upsert_meta_locked("schema_version", str(SCHEMA_VERSION))
            if self.get_meta("created_at") is None:
                self._upsert_meta_locked("created_at", utc_now())

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row else None

    def _upsert_meta_locked(self, key: str, value: str) -> None:
        self.conn.execute(
            """
            INSERT INTO meta(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )

    def set_meta(self, key: str, value: str) -> None:
        with self._immediate_transaction():
            self._upsert_meta_locked(key, value)

    def set_meta_many(self, values: Mapping[str, str]) -> None:
        with self._immediate_transaction():
            for key, value in values.items():
                self._upsert_meta_locked(key, value)

    def bind_project(self, project_dir: Path) -> None:
        resolved_project_dir = project_dir.resolve()
        project_value = str(resolved_project_dir)
        with self._immediate_transaction():
            previous_project = self.get_meta("project_dir")
            if previous_project is not None:
                previous_path = Path(previous_project)
                if (
                    not previous_project.strip()
                    or not previous_path.is_absolute()
                    or previous_path.resolve() != resolved_project_dir
                ):
                    raise ValueError(
                        "该输出目录已经绑定到另一个 project_dir："
                        f"\n现有：{previous_project}\n当前：{project_dir}"
                    )
                return

            has_tasks = self.conn.execute(
                "SELECT 1 FROM tasks LIMIT 1"
            ).fetchone()
            has_attempts = self.conn.execute(
                "SELECT 1 FROM attempts LIMIT 1"
            ).fetchone()
            has_non_bootstrap_meta = self.conn.execute(
                """
                SELECT 1 FROM meta
                WHERE key NOT IN ('schema_version', 'created_at')
                LIMIT 1
                """
            ).fetchone()
            if has_tasks or has_attempts or has_non_bootstrap_meta:
                raise ValueError(
                    "状态数据库缺少 project_dir，但已存在项目状态；拒绝自动绑定。"
                )
            self._upsert_meta_locked("project_dir", project_value)

    def adopt_thread_id(
        self,
        *,
        expected_old_thread_id: str | None,
        new_thread_id: str,
    ) -> None:
        new_thread_id = new_thread_id.strip()
        if not new_thread_id:
            raise RuntimeError("新建 Thread 未返回有效的 Thread ID。")
        if new_thread_id == expected_old_thread_id:
            raise RuntimeError("新建 Thread 返回了原有 Thread ID；拒绝切换。")

        with self._immediate_transaction():
            current_thread_id = self.get_meta("thread_id")
            if current_thread_id != expected_old_thread_id:
                raise ThreadAdoptionConflict(
                    "新 Thread 创建期间，本地 Thread ID 已发生变化；"
                    "为避免在错误 Thread 上启动 Turn，拒绝切换。"
                )
            self._upsert_meta_locked("thread_id", new_thread_id)

    def delete_meta(self, key: str) -> None:
        with self._immediate_transaction():
            self.conn.execute("DELETE FROM meta WHERE key = ?", (key,))

    def _sync_tasks_locked(self, tasks: Sequence[TaskSpec]) -> None:
        now = utc_now()
        for task in tasks:
            row = self.task(task.task_id)
            if row is None:
                self.conn.execute(
                    """
                    INSERT INTO tasks(
                        task_id, ordinal, enabled, document, function_name, source,
                        custom_prompt, fingerprint, status, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task.task_id,
                        task.ordinal,
                        int(task.enabled),
                        task.document,
                        task.function,
                        task.source,
                        task.custom_prompt,
                        task.fingerprint,
                        "PENDING" if task.enabled else "DISABLED",
                        now,
                        now,
                    ),
                )
                continue

            fingerprint_changed = row["fingerprint"] != task.fingerprint
            enabled_changed = bool(row["enabled"]) != task.enabled
            has_unresolved_attempt = bool(self.unresolved_attempts(task.task_id)) if (
                fingerprint_changed or enabled_changed
            ) else False

            if fingerprint_changed:
                if (
                    _is_unresolved_task_status(row["status"])
                    or has_unresolved_attempt
                ):
                    raise ValueError(
                        f"未完成任务 {task.task_id!r} 的 document/function/source/prompt "
                        "发生变化。为避免重复修改，必须先恢复原任务内容并完成对账；"
                        "状态未知的 Turn 不能通过 --retry-incomplete 自动放行。"
                    )
                if row["status"] == "SUCCEEDED":
                    raise ValueError(
                        f"已成功任务 {task.task_id!r} 的内容发生变化。"
                        "请恢复原内容，或为修改后的任务使用新的 id。"
                    )
                self.conn.execute(
                    """
                    UPDATE tasks SET ordinal=?, enabled=?, document=?, function_name=?,
                        source=?, custom_prompt=?, fingerprint=?, status=?, updated_at=?
                    WHERE task_id=?
                    """,
                    (
                        task.ordinal,
                        int(task.enabled),
                        task.document,
                        task.function,
                        task.source,
                        task.custom_prompt,
                        task.fingerprint,
                        "PENDING" if task.enabled else "DISABLED",
                        now,
                        task.task_id,
                    ),
                )
            else:
                if (
                    enabled_changed
                    and (
                        _is_unresolved_task_status(row["status"])
                        or has_unresolved_attempt
                    )
                ):
                    raise ValueError(
                        f"未完成任务 {task.task_id!r} 的 enabled 发生变化。"
                        "请先完成对账，不能用禁用任务来跳过未知 Turn。"
                    )
                next_status = row["status"]
                if (
                    not task.enabled
                    and row["status"] in NON_BLOCKING_TASK_STATUSES
                    and row["status"] != "SUCCEEDED"
                ):
                    next_status = "DISABLED"
                elif task.enabled and row["status"] == "DISABLED":
                    next_status = "PENDING"
                self.conn.execute(
                    """
                    UPDATE tasks SET ordinal=?, enabled=?, status=?, updated_at=?
                    WHERE task_id=?
                    """,
                    (task.ordinal, int(task.enabled), next_status, now, task.task_id),
                )

    def sync_tasks(self, tasks: Sequence[TaskSpec]) -> None:
        with self._immediate_transaction():
            self._sync_tasks_locked(tasks)

    def task(self, task_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()

    def all_tasks_with_latest_attempt(self) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT t.*, a.thread_id, a.turn_id, a.status AS attempt_status,
                       a.started_at, a.completed_at, a.duration_ms, a.prompt,
                       a.final_response, a.error_json, a.usage_json,
                       a.agents_snapshot_json, a.prompt_file, a.event_file,
                       a.response_file
                FROM tasks t
                LEFT JOIN attempts a
                  ON a.task_id = t.task_id AND a.attempt_no = t.latest_attempt
                ORDER BY t.ordinal, t.task_id
                """
            )
        )

    def latest_attempt(self, task_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT a.* FROM attempts a
            JOIN tasks t ON t.task_id = a.task_id
            WHERE a.task_id = ? AND a.attempt_no = t.latest_attempt
            """,
            (task_id,),
        ).fetchone()

    def next_attempt_no(self, task_id: str) -> int:
        row = self.task(task_id)
        if row is None:
            raise KeyError(task_id)
        return int(row["latest_attempt"]) + 1

    def start_attempt(
        self,
        task: TaskSpec,
        *,
        attempt_no: int,
        prompt: str,
        agents_snapshot: list[dict[str, Any]],
        prompt_file: str,
        event_file: str,
        response_file: str,
    ) -> None:
        attempt_no = require_sqlite_attempt_no(attempt_no)
        now = utc_now()
        startable_statuses = sorted(STARTABLE_TASK_STATUSES)
        startable_placeholders = ", ".join("?" for _ in startable_statuses)
        unresolved_attempt_condition, unresolved_attempt_parameters = (
            _unresolved_attempt_condition("a")
        )
        with self._immediate_transaction():
            if not task.enabled:
                raise TurnCheckpointConflict(
                    f"任务 {task.task_id!r} 的 TaskSpec enabled=false；拒绝启动 Attempt。"
                )
            artifact_paths = (prompt_file, event_file, response_file)
            artifact_owner = self.conn.execute(
                """
                SELECT task_id, attempt_no FROM attempts
                WHERE prompt_file IN (?, ?, ?)
                   OR event_file IN (?, ?, ?)
                   OR response_file IN (?, ?, ?)
                LIMIT 1
                """,
                artifact_paths * 3,
            ).fetchone()
            if artifact_owner is not None:
                raise TurnCheckpointConflict(
                    f"任务 {task.task_id!r} Attempt {attempt_no} 的工件路径已由 "
                    f"{artifact_owner['task_id']!r} Attempt "
                    f"{artifact_owner['attempt_no']} 占用；拒绝复用历史工件。"
                )
            task_cursor = self.conn.execute(
                f"""
                UPDATE tasks
                SET status='TURN_NOT_REQUESTED', latest_attempt=?, updated_at=?
                WHERE task_id=? AND enabled=1 AND fingerprint=?
                  AND status IN ({startable_placeholders})
                  AND latest_attempt=?
                  AND NOT EXISTS (
                    SELECT 1 FROM attempts a
                    WHERE a.task_id=tasks.task_id
                      AND {unresolved_attempt_condition}
                  )
                """,
                (
                    attempt_no,
                    now,
                    task.task_id,
                    task.fingerprint,
                    *startable_statuses,
                    attempt_no - 1,
                    *unresolved_attempt_parameters,
                ),
            )
            if task_cursor.rowcount != 1:
                raise TurnCheckpointConflict(
                    f"任务 {task.task_id!r} Attempt {attempt_no} 无法从当前任务快照启动；"
                    "任务状态、enabled、fingerprint、latest_attempt "
                    "或既有 Attempt 状态已变化。"
                )
            self.conn.execute(
                """
                INSERT INTO attempts(
                    task_id, attempt_no, status, started_at, prompt,
                    agents_snapshot_json, prompt_file, event_file, response_file
                ) VALUES(?, ?, 'TURN_NOT_REQUESTED', ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.task_id,
                    attempt_no,
                    now,
                    prompt,
                    json.dumps(agents_snapshot, ensure_ascii=False),
                    prompt_file,
                    event_file,
                    response_file,
                ),
            )

    def mark_turn_start_requested(
        self,
        task_id: str,
        attempt_no: int,
        *,
        thread_id: str,
    ) -> None:
        now = utc_now()
        with self._immediate_transaction():
            attempt_cursor = self.conn.execute(
                """
                UPDATE attempts
                SET thread_id=?, status='TURN_START_REQUESTED'
                WHERE task_id=? AND attempt_no=?
                  AND status='TURN_NOT_REQUESTED' AND turn_id IS NULL
                """,
                (thread_id, task_id, attempt_no),
            )
            task_cursor = self.conn.execute(
                """
                UPDATE tasks SET status='TURN_START_REQUESTED', updated_at=?
                WHERE task_id=? AND latest_attempt=?
                  AND status='TURN_NOT_REQUESTED'
                """,
                (now, task_id, attempt_no),
            )
            if attempt_cursor.rowcount != 1 or task_cursor.rowcount != 1:
                raise TurnCheckpointConflict(
                    f"任务 {task_id!r} 无法进入 TURN_START_REQUESTED；"
                    "检查点状态已变化。"
                )

    def mark_turn_started(
        self, task_id: str, attempt_no: int, *, thread_id: str, turn_id: str
    ) -> None:
        turn_id = required_turn_id(
            turn_id,
            source="Store.mark_turn_started turn_id",
        )
        now = utc_now()
        with self._immediate_transaction():
            attempt_cursor = self.conn.execute(
                """
                UPDATE attempts
                SET thread_id=?, turn_id=?, status='RUNNING'
                WHERE task_id=? AND attempt_no=?
                  AND thread_id=? AND status='TURN_START_REQUESTED'
                  AND turn_id IS NULL
                """,
                (thread_id, turn_id, task_id, attempt_no, thread_id),
            )
            task_cursor = self.conn.execute(
                """
                UPDATE tasks SET status='RUNNING', updated_at=?
                WHERE task_id=? AND latest_attempt=?
                  AND status='TURN_START_REQUESTED'
                """,
                (now, task_id, attempt_no),
            )
            if attempt_cursor.rowcount != 1 or task_cursor.rowcount != 1:
                raise TurnCheckpointConflict(
                    f"任务 {task_id!r} 无法记录 Turn ID；检查点状态已变化。"
                )

    def finish_attempt(
        self,
        task_id: str,
        attempt_no: int,
        *,
        expected_task_status: str,
        expected_attempt_status: str,
        expected_turn_id: str | None,
        task_status: str,
        attempt_status: str,
        final_response: str | None,
        error: Any,
        usage: Any,
        duration_ms: int | None,
        publish_response: Callable[[], None] | None = None,
    ) -> None:
        now = utc_now()
        with self._immediate_transaction():
            attempt_cursor = self.conn.execute(
                """
                UPDATE attempts
                SET status=?, completed_at=?, duration_ms=?, final_response=?,
                    error_json=?, usage_json=?
                WHERE task_id=? AND attempt_no=? AND status=?
                  AND turn_id IS ?
                """,
                (
                    attempt_status,
                    now,
                    duration_ms,
                    final_response,
                    json.dumps(jsonable(error), ensure_ascii=False) if error is not None else None,
                    json.dumps(jsonable(usage), ensure_ascii=False) if usage is not None else None,
                    task_id,
                    attempt_no,
                    expected_attempt_status,
                    expected_turn_id,
                ),
            )
            task_cursor = self.conn.execute(
                """
                UPDATE tasks SET status=?, updated_at=?
                WHERE task_id=? AND latest_attempt=? AND status=?
                """,
                (
                    task_status,
                    now,
                    task_id,
                    attempt_no,
                    expected_task_status,
                ),
            )
            if attempt_cursor.rowcount != 1 or task_cursor.rowcount != 1:
                raise TurnCheckpointConflict(
                    f"任务 {task_id!r} Attempt {attempt_no} 无法从预期检查点完成；"
                    "任务状态、Attempt 状态、Turn ID 或 latest_attempt 已变化。"
                )
            # 候选响应已在事务外写入并 fsync。只有两条 CAS 都成功后才发布；
            # 发布异常会离开事务上下文并回滚数据库更新。
            if publish_response is not None:
                publish_response()

    def _validate_set_pending_locked(self, task_id: str) -> None:
        row = self.task(task_id)
        if row is None:
            raise ValueError(f"未知任务 id：{task_id}")
        if not row["enabled"]:
            raise ValueError(f"任务 {task_id!r} 当前 enabled=false。")
        if row["status"] not in RERUNNABLE_TASK_STATUSES:
            allowed = ", ".join(sorted(RERUNNABLE_TASK_STATUSES))
            raise ValueError(
                f"任务 {task_id!r} 状态是 {row['status']}；"
                f"--rerun 仅允许可安全重跑状态：{allowed}。"
                "未完成任务必须先对账，不能用 --rerun 跳过保护。"
            )
        if self.unresolved_attempts(task_id):
            raise ValueError(
                f"任务 {task_id!r} 存在未完成 Attempt 或状态已变化；"
                "为避免重复 Turn，拒绝 --rerun。"
            )

    def _set_pending_locked(self, task_id: str, *, now: str) -> None:
        attempt_condition, attempt_parameters = _unresolved_attempt_condition()
        rerunnable_statuses = sorted(RERUNNABLE_TASK_STATUSES)
        rerunnable_placeholders = ", ".join("?" for _ in rerunnable_statuses)
        parameters: tuple[Any, ...] = (
            now,
            task_id,
            *rerunnable_statuses,
            task_id,
            *attempt_parameters,
        )
        cursor = self.conn.execute(
            f"""
            UPDATE tasks SET status='PENDING', updated_at=?
            WHERE task_id=? AND enabled=1
              AND status IN ({rerunnable_placeholders})
              AND NOT EXISTS (
                SELECT 1 FROM attempts
                WHERE task_id=? AND {attempt_condition}
              )
            """,
            parameters,
        )
        if cursor.rowcount != 1:
            raise RuntimeError(
                f"任务 {task_id!r} 的 --rerun 状态在事务中发生变化；拒绝提交。"
            )

    def set_pending(self, task_id: str) -> None:
        with self._immediate_transaction():
            self._validate_set_pending_locked(task_id)
            self._set_pending_locked(task_id, now=utc_now())

    def _validate_incomplete_retry_locked(self, task_id: str) -> int:
        row = self.task(task_id)
        if row is None:
            raise ValueError(f"未知任务 id：{task_id}")
        attempt = self.latest_attempt(task_id)
        unresolved_attempts = self.unresolved_attempts(task_id)
        can_retry = (
            bool(row["enabled"])
            and row["status"] == "TURN_NOT_REQUESTED"
            and attempt is not None
            and attempt["status"] == "TURN_NOT_REQUESTED"
            and attempt["thread_id"] is None
            and attempt["turn_id"] is None
            and len(unresolved_attempts) == 1
            and unresolved_attempts[0]["attempt_no"] == attempt["attempt_no"]
        )
        if not can_retry:
            attempt_status = attempt["status"] if attempt is not None else "MISSING"
            raise ValueError(
                f"任务 {task_id!r} 不能使用 --retry-incomplete："
                f"task={row['status']}，attempt={attempt_status}。"
                "该参数仅允许重试 task/attempt 均为 TURN_NOT_REQUESTED、"
                "thread_id/turn_id 均为空且没有其他未完成 Attempt 的请求前中断。"
                "当前状态不能由本地记录证明旧 Turn 已终止，拒绝启动新 Turn。"
            )
        return int(attempt["attempt_no"])

    def _mark_incomplete_for_retry_locked(
        self,
        task_id: str,
        attempt_no: int,
        *,
        now: str,
    ) -> None:
        attempt_cursor = self.conn.execute(
            """
            UPDATE attempts
            SET status='ABANDONED_BEFORE_REQUEST', completed_at=?
            WHERE task_id=? AND attempt_no=? AND status='TURN_NOT_REQUESTED'
              AND thread_id IS NULL AND turn_id IS NULL
            """,
            (now, task_id, attempt_no),
        )
        task_cursor = self.conn.execute(
            """
            UPDATE tasks SET status='PENDING', updated_at=?
            WHERE task_id=? AND latest_attempt=?
              AND status='TURN_NOT_REQUESTED'
              AND enabled=1
            """,
            (now, task_id, attempt_no),
        )
        if attempt_cursor.rowcount != 1 or task_cursor.rowcount != 1:
            raise RuntimeError(
                f"任务 {task_id!r} 的安全重试检查点已变化；拒绝重试。"
            )

    def mark_incomplete_for_retry(self, task_id: str) -> None:
        with self._immediate_transaction():
            attempt_no = self._validate_incomplete_retry_locked(task_id)
            self._mark_incomplete_for_retry_locked(
                task_id,
                attempt_no,
                now=utc_now(),
            )

    def apply_retry_options(
        self,
        *,
        rerun: Sequence[str],
        retry_incomplete: Sequence[str],
    ) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
        rerun_ids = list(dict.fromkeys(rerun))
        retry_ids = list(dict.fromkeys(retry_incomplete))
        rerun_set = set(rerun_ids)
        overlap = [task_id for task_id in retry_ids if task_id in rerun_set]
        if overlap:
            formatted = ", ".join(repr(task_id) for task_id in overlap)
            raise ValueError(
                "同一任务不能同时使用 --rerun 和 --retry-incomplete："
                f"{formatted}。"
            )

        with self._immediate_transaction():
            retry_plans = [
                (task_id, self._validate_incomplete_retry_locked(task_id))
                for task_id in retry_ids
            ]
            retry_task_ids = {task_id for task_id, _attempt_no in retry_plans}
            retry_attempts = set(retry_plans)
            unresolved = [
                row
                for row in self.unresolved_tasks()
                if row["task_id"] not in retry_task_ids
            ]
            unresolved_attempts = [
                row
                for row in self.unresolved_attempts()
                if (row["task_id"], int(row["attempt_no"])) not in retry_attempts
            ]
            if unresolved or unresolved_attempts:
                return unresolved, unresolved_attempts

            for task_id in rerun_ids:
                self._validate_set_pending_locked(task_id)

            now = utc_now()
            for task_id, attempt_no in retry_plans:
                self._mark_incomplete_for_retry_locked(
                    task_id,
                    attempt_no,
                    now=now,
                )
            for task_id in rerun_ids:
                self._set_pending_locked(task_id, now=now)
            return [], []

    def unresolved_tasks(self) -> list[sqlite3.Row]:
        task_condition, parameters = _outside_status_condition(
            NON_BLOCKING_TASK_STATUSES
        )
        return list(
            self.conn.execute(
                f"""
                SELECT * FROM tasks
                WHERE {task_condition}
                ORDER BY ordinal
                """,
                parameters,
            )
        )

    def unresolved_attempts(self, task_id: str | None = None) -> list[sqlite3.Row]:
        attempt_condition, parameters = _unresolved_attempt_condition("a")
        task_filter = " AND a.task_id=?" if task_id is not None else ""
        query_parameters: tuple[Any, ...] = parameters + (
            (task_id,) if task_id is not None else ()
        )
        return list(
            self.conn.execute(
                f"""
                SELECT a.task_id, a.attempt_no, a.status, a.thread_id, a.turn_id,
                       t.ordinal
                FROM attempts a
                JOIN tasks t ON t.task_id = a.task_id
                WHERE {attempt_condition}{task_filter}
                ORDER BY t.ordinal, a.attempt_no
                """,
                query_parameters,
            )
        )


class RunLock:
    def __init__(self, path: Path, *, force: bool = False) -> None:
        self.path = path
        self.force = force
        self.acquired = False
        self._descriptor: int | None = None

    @staticmethod
    def _read_text(descriptor: int) -> str:
        data = os.pread(descriptor, RUN_LOCK_MAX_BYTES + 1, 0)
        if len(data) > RUN_LOCK_MAX_BYTES:
            raise ValueError("运行锁内容过大")
        return data.decode("utf-8")

    @classmethod
    def _read_record(cls, descriptor: int) -> tuple[str, dict[str, Any]]:
        text = cls._read_text(descriptor)
        try:
            record = json.loads(text)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError("运行锁不是有效的 UTF-8 JSON") from exc
        if not isinstance(record, dict):
            raise ValueError("运行锁 JSON 必须是对象")
        return text, record

    @staticmethod
    def _valid_pid(value: Any) -> bool:
        return (
            isinstance(value, int)
            and not isinstance(value, bool)
            and 0 < value <= RUN_LOCK_MAX_PID
        )

    @staticmethod
    def _same_file(path: Path, descriptor: int) -> bool:
        try:
            path_stat = os.lstat(path)
            descriptor_stat = os.fstat(descriptor)
        except FileNotFoundError:
            return False
        return (
            stat.S_ISREG(path_stat.st_mode)
            and stat.S_ISREG(descriptor_stat.st_mode)
            and descriptor_stat.st_nlink == 1
            and (path_stat.st_dev, path_stat.st_ino)
            == (descriptor_stat.st_dev, descriptor_stat.st_ino)
        )

    @staticmethod
    def _write_record(descriptor: int, record: Mapping[str, Any]) -> None:
        payload = json.dumps(record, ensure_ascii=False).encode("utf-8")
        os.ftruncate(descriptor, 0)
        os.lseek(descriptor, 0, os.SEEK_SET)
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("写入运行锁失败")
            remaining = remaining[written:]
        os.fsync(descriptor)

    def _open_candidate(self) -> tuple[int, bool]:
        flags = (
            os.O_RDWR
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            return (
                os.open(self.path, flags | os.O_CREAT | os.O_EXCL, 0o600),
                True,
            )
        except FileExistsError:
            return os.open(self.path, flags), False

    def _busy_error(self, descriptor: int) -> RuntimeError:
        try:
            owner = self._read_text(descriptor)
        except (OSError, ValueError, UnicodeDecodeError):
            owner = "未知"
        return RuntimeError(
            f"检测到正在运行的另一个批处理：{self.path}\n"
            f"锁信息：{owner}\n"
            "--force-unlock 不会覆盖仍被进程持有的运行锁。"
        )

    def _validate_existing_record(self, descriptor: int) -> None:
        try:
            _text, record = self._read_record(descriptor)
        except ValueError as exc:
            raise RuntimeError(
                f"运行锁内容无法安全验证：{self.path}\n"
                "拒绝自动覆盖；请先确认没有旧版批处理仍在运行。"
            ) from exc

        if "protocol" in record:
            if record.get("protocol") != RUN_LOCK_PROTOCOL:
                raise RuntimeError(
                    f"不支持的运行锁协议：{record.get('protocol')!r}"
                )
            if not self._valid_pid(record.get("pid")):
                raise RuntimeError("运行锁中的 PID 无效；拒绝自动覆盖。")
            if not isinstance(record.get("owner_token"), str) or not record[
                "owner_token"
            ]:
                raise RuntimeError("运行锁中的 owner_token 无效；拒绝自动覆盖。")
            return

        if not self.force:
            raise RuntimeError(
                f"检测到旧版运行锁：{self.path}\n"
                "只有确认旧 PID 已退出后，才能使用 --force-unlock 迁移。"
            )

        pid = record.get("pid")
        if not self._valid_pid(pid):
            raise RuntimeError("旧版运行锁中的 PID 无效；拒绝强制迁移。")
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        except PermissionError as exc:
            raise RuntimeError(
                f"无法确认旧版运行锁 PID {pid} 是否已退出；拒绝强制迁移。"
            ) from exc
        except OverflowError as exc:
            raise RuntimeError(
                f"旧版运行锁 PID {pid} 超出系统可检查范围；拒绝强制迁移。"
            ) from exc
        except OSError as exc:
            raise RuntimeError(
                f"检查旧版运行锁 PID {pid} 失败；拒绝强制迁移。"
            ) from exc
        raise RuntimeError(
            f"旧版运行锁 PID {pid} 仍在运行；--force-unlock 不会删除活跃锁。"
        )

    def __enter__(self) -> "RunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        for _attempt in range(RUN_LOCK_MAX_RETRIES):
            try:
                descriptor, created = self._open_candidate()
            except FileNotFoundError:
                continue

            locked = False
            keep_descriptor = False
            try:
                descriptor_stat = os.fstat(descriptor)
                if not stat.S_ISREG(descriptor_stat.st_mode):
                    raise RuntimeError(f"运行锁不是普通文件：{self.path}")
                if descriptor_stat.st_nlink != 1:
                    raise RuntimeError(f"运行锁存在额外硬链接：{self.path}")
                try:
                    # 创建者已经通过 O_EXCL 赢得 pathname；若跟随者恰好先拿到
                    # 空 inode 的 flock，创建者等待其快速校验失败后继续初始化，
                    # 避免双方都失败并永久遗留空锁。
                    lock_operation = fcntl.LOCK_EX
                    if not created:
                        lock_operation |= fcntl.LOCK_NB
                    fcntl.flock(descriptor, lock_operation)
                    locked = True
                except OSError as exc:
                    if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                        raise
                    raise self._busy_error(descriptor) from exc
                os.fchmod(descriptor, 0o600)

                if not self._same_file(self.path, descriptor):
                    continue
                if not created:
                    self._validate_existing_record(descriptor)
                    # PID 检查期间 pathname 可能已被替换；只能迁移同一 inode。
                    if not self._same_file(self.path, descriptor):
                        continue

                owner_token = secrets.token_hex(16)
                self._write_record(
                    descriptor,
                    {
                        "protocol": RUN_LOCK_PROTOCOL,
                        "pid": os.getpid(),
                        "started_at": utc_now(),
                        "owner_token": owner_token,
                    },
                )
                if not self._same_file(self.path, descriptor):
                    continue

                self._descriptor = descriptor
                self.acquired = True
                keep_descriptor = True
                return self
            finally:
                if not keep_descriptor:
                    if locked:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    os.close(descriptor)

        raise RuntimeError(
            f"运行锁在检查期间反复变化；拒绝启动批处理：{self.path}"
        )

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        self.acquired = False
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


def validated_output_write_relative(
    stored_path: str | Path,
    *,
    field: str,
    expected_directory: str | None = None,
    expected_leaf: str | None = None,
) -> Path:
    if (expected_directory is None) == (expected_leaf is None):
        raise RuntimeError(
            "描述符写入必须且只能指定 expected_directory 或 expected_leaf。"
        )
    relative = Path(str(stored_path))
    if (
        relative.is_absolute()
        or not relative.parts
        or not relative.name
        or ".." in relative.parts
    ):
        raise ValueError(f"{field} 不是安全的输出相对路径：{stored_path!r}")
    if expected_directory is not None and (
        len(relative.parts) < 2
        or relative.parts[0] != expected_directory
    ):
        raise ValueError(
            f"{field} 必须位于 {expected_directory}/：{stored_path!r}"
        )
    if expected_leaf is not None and (
        len(relative.parts) != 1 or relative.name != expected_leaf
    ):
        raise ValueError(
            f"{field} 必须是 output_dir/{expected_leaf}：{stored_path!r}"
        )
    return relative


def validated_output_artifact_relative(
    stored_path: str | Path,
    *,
    directory: str,
    field: str,
) -> Path:
    return validated_output_write_relative(
        stored_path,
        field=field,
        expected_directory=directory,
    )


def open_descriptor(
    path: Any,
    flags: int,
    mode: int = 0o777,
    *,
    dir_fd: int | None = None,
) -> int:
    if dir_fd is None:
        return os.open(path, flags, mode)
    return os.open(path, flags, mode, dir_fd=dir_fd)


def stat_descriptor_path(path: Any, *, dir_fd: int) -> os.stat_result:
    return os.stat(path, dir_fd=dir_fd, follow_symlinks=False)


def replace_descriptor_path(
    source: str,
    destination: str,
    *,
    source_dir_fd: int,
    destination_dir_fd: int,
) -> None:
    os.replace(
        source,
        destination,
        src_dir_fd=source_dir_fd,
        dst_dir_fd=destination_dir_fd,
    )


def unlink_descriptor_path(path: str, *, dir_fd: int) -> None:
    os.unlink(path, dir_fd=dir_fd)


def validate_private_directory_descriptor(
    descriptor: int,
    *,
    path: Path,
    field: str,
    label: str,
    expected_uid: int | None,
    normalize_permissions: bool = False,
) -> None:
    opened_stat = os.fstat(descriptor)
    if not stat.S_ISDIR(opened_stat.st_mode):
        raise ValueError(f"{field} 的 {label} 不是目录：{path}")
    if expected_uid is not None and opened_stat.st_uid != expected_uid:
        raise ValueError(f"{field} 的 {label} 不属于当前用户：{path}")
    mode = stat.S_IMODE(opened_stat.st_mode)
    if mode & 0o077 or mode & 0o700 != 0o700:
        if not normalize_permissions:
            raise ValueError(
                f"{field} 的 {label} 权限必须是 0700，"
                f"当前为 {mode:04o}：{path}"
            )
        os.fchmod(descriptor, 0o700)
        normalized_stat = os.fstat(descriptor)
        normalized_mode = stat.S_IMODE(normalized_stat.st_mode)
        if (
            not stat.S_ISDIR(normalized_stat.st_mode)
            or (
                expected_uid is not None
                and normalized_stat.st_uid != expected_uid
            )
            or normalized_mode & 0o077
            or normalized_mode & 0o700 != 0o700
        ):
            raise ValueError(
                f"{field} 的 {label} 无法收紧为 0700：{path}"
            )


@dataclass(slots=True)
class OutputParentAnchor:
    output_root: Path
    relative: Path
    field: str
    root_descriptor: int
    parent_descriptor: int
    directory_links: tuple[tuple[int, str, int], ...]

    @property
    def path(self) -> Path:
        return self.output_root / self.relative

    def verify_path_identity(self) -> None:
        unsafe_message = f"{self.field} 父路径在发布前已发生变化：{self.path}"
        try:
            root_path_stat = os.lstat(self.output_root)
            root_descriptor_stat = os.fstat(self.root_descriptor)
        except FileNotFoundError as exc:
            raise ValueError(unsafe_message) from exc
        if (
            not stat.S_ISDIR(root_path_stat.st_mode)
            or not stat.S_ISDIR(root_descriptor_stat.st_mode)
            or (root_path_stat.st_dev, root_path_stat.st_ino)
            != (root_descriptor_stat.st_dev, root_descriptor_stat.st_ino)
        ):
            raise ValueError(unsafe_message)

        for parent_descriptor, component, child_descriptor in self.directory_links:
            try:
                path_stat = stat_descriptor_path(
                    component,
                    dir_fd=parent_descriptor,
                )
                descriptor_stat = os.fstat(child_descriptor)
            except FileNotFoundError as exc:
                raise ValueError(unsafe_message) from exc
            if (
                not stat.S_ISDIR(path_stat.st_mode)
                or not stat.S_ISDIR(descriptor_stat.st_mode)
                or (path_stat.st_dev, path_stat.st_ino)
                != (descriptor_stat.st_dev, descriptor_stat.st_ino)
            ):
                raise ValueError(unsafe_message)


@contextmanager
def open_output_parent_anchor(
    output_root: Path,
    stored_path: str | Path,
    *,
    field: str,
    expected_directory: str | None = None,
    expected_leaf: str | None = None,
    fsync_created_parents: bool = False,
) -> Iterator[OutputParentAnchor]:
    relative = validated_output_write_relative(
        stored_path,
        field=field,
        expected_directory=expected_directory,
        expected_leaf=expected_leaf,
    )
    path = output_root / relative
    if not output_root.is_absolute():
        raise ValueError(f"{field} 的 output_dir 必须是绝对路径：{output_root}")

    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory_only = getattr(os, "O_DIRECTORY", 0)
    non_blocking = getattr(os, "O_NONBLOCK", 0)
    supports_dir_fd = getattr(os, "supports_dir_fd", set())
    supports_follow_symlinks = getattr(os, "supports_follow_symlinks", set())
    required_dir_fd_functions = {os.open, os.mkdir, os.stat}
    if (
        not no_follow
        or not directory_only
        or not non_blocking
        or not required_dir_fd_functions.issubset(supports_dir_fd)
        or os.stat not in supports_follow_symlinks
    ):
        raise RuntimeError("当前平台不支持安全的输出描述符路径操作。")

    directory_flags = (
        os.O_RDONLY
        | directory_only
        | no_follow
        | non_blocking
        | getattr(os, "O_CLOEXEC", 0)
    )
    unsafe_errnos = {errno.ELOOP, errno.ENOTDIR, errno.EISDIR, errno.ENXIO}
    unsafe_message = f"{field} 路径在描述符打开时不安全：{path}"
    expected_uid = os.geteuid() if hasattr(os, "geteuid") else None

    with ExitStack() as directory_descriptors:
        try:
            root_descriptor = open_descriptor(output_root, directory_flags)
        except OSError as exc:
            if exc.errno in unsafe_errnos:
                raise ValueError(unsafe_message) from exc
            raise
        directory_descriptors.callback(os.close, root_descriptor)
        validate_private_directory_descriptor(
            root_descriptor,
            path=path,
            field=field,
            label="output_dir",
            expected_uid=expected_uid,
        )

        current_descriptor = root_descriptor
        directory_links: list[tuple[int, str, int]] = []
        for component in relative.parts[:-1]:
            created = False
            try:
                next_descriptor = open_descriptor(
                    component,
                    directory_flags,
                    dir_fd=current_descriptor,
                )
            except FileNotFoundError:
                try:
                    os.mkdir(component, 0o700, dir_fd=current_descriptor)
                    created = True
                except FileExistsError:
                    pass
                try:
                    next_descriptor = open_descriptor(
                        component,
                        directory_flags,
                        dir_fd=current_descriptor,
                    )
                except OSError as exc:
                    if exc.errno in unsafe_errnos:
                        raise ValueError(unsafe_message) from exc
                    raise
            except OSError as exc:
                if exc.errno in unsafe_errnos:
                    raise ValueError(unsafe_message) from exc
                raise

            directory_descriptors.callback(os.close, next_descriptor)
            validate_private_directory_descriptor(
                next_descriptor,
                path=path,
                field=field,
                label="父目录",
                expected_uid=expected_uid,
                normalize_permissions=True,
            )
            if created and fsync_created_parents:
                os.fsync(current_descriptor)
            directory_links.append(
                (current_descriptor, component, next_descriptor)
            )
            current_descriptor = next_descriptor

        anchor = OutputParentAnchor(
            output_root=output_root,
            relative=relative,
            field=field,
            root_descriptor=root_descriptor,
            parent_descriptor=current_descriptor,
            directory_links=tuple(directory_links),
        )
        anchor.verify_path_identity()
        yield anchor


@contextmanager
def staged_atomic_write_text(
    output_root: Path,
    stored_path: str | Path,
    content: str,
    *,
    field: str,
    expected_directory: str | None = None,
    expected_leaf: str | None = None,
    fsync_parent: bool = False,
) -> Iterator[Callable[[], None]]:
    """Stage and publish a private text file within one anchored parent FD."""
    payload = content.encode("utf-8")
    supports_dir_fd = getattr(os, "supports_dir_fd", set())
    if os.unlink not in supports_dir_fd or os.rename not in supports_dir_fd:
        raise RuntimeError("当前平台不支持安全的描述符相对原子发布。")

    with open_output_parent_anchor(
        output_root,
        stored_path,
        field=field,
        expected_directory=expected_directory,
        expected_leaf=expected_leaf,
        fsync_created_parents=fsync_parent,
    ) as anchor:
        candidate_flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        expected_uid = os.geteuid() if hasattr(os, "geteuid") else None
        candidate_descriptor: int | None = None
        candidate_name = ""
        for _attempt in range(ATOMIC_TEMP_MAX_RETRIES):
            candidate_name = f".codex-batch-{secrets.token_hex(16)}.tmp"
            try:
                candidate_descriptor = open_descriptor(
                    candidate_name,
                    candidate_flags,
                    0o600,
                    dir_fd=anchor.parent_descriptor,
                )
                break
            except FileExistsError:
                continue
        if candidate_descriptor is None:
            raise RuntimeError(
                f"无法创建不冲突的私有候选文件：{anchor.path}"
            )

        published = False
        active = True
        primary_error: BaseException | None = None
        try:
            candidate_stat = os.fstat(candidate_descriptor)
            if not stat.S_ISREG(candidate_stat.st_mode):
                raise ValueError(f"{field} 候选不是普通文件：{anchor.path}")
            if expected_uid is not None and candidate_stat.st_uid != expected_uid:
                raise ValueError(f"{field} 候选不属于当前用户：{anchor.path}")
            if candidate_stat.st_nlink != 1:
                raise ValueError(f"{field} 候选存在额外硬链接：{anchor.path}")
            os.fchmod(candidate_descriptor, 0o600)

            remaining = memoryview(payload)
            while remaining:
                written = os.write(candidate_descriptor, remaining)
                if written <= 0:
                    raise OSError(f"写入候选文件失败：{anchor.path}")
                remaining = remaining[written:]
            os.fsync(candidate_descriptor)

            def publish() -> None:
                nonlocal published
                if not active:
                    raise RuntimeError(f"候选文件上下文已经结束：{anchor.path}")
                if published:
                    raise RuntimeError(f"候选文件已经发布：{anchor.path}")
                anchor.verify_path_identity()
                descriptor_stat = os.fstat(candidate_descriptor)
                try:
                    candidate_path_stat = stat_descriptor_path(
                        candidate_name,
                        dir_fd=anchor.parent_descriptor,
                    )
                except FileNotFoundError as exc:
                    raise ValueError(
                        f"{field} 候选路径在发布前已消失：{anchor.path}"
                    ) from exc
                if (
                    not stat.S_ISREG(descriptor_stat.st_mode)
                    or not stat.S_ISREG(candidate_path_stat.st_mode)
                    or (
                        expected_uid is not None
                        and descriptor_stat.st_uid != expected_uid
                    )
                    or (
                        expected_uid is not None
                        and candidate_path_stat.st_uid != expected_uid
                    )
                    or descriptor_stat.st_nlink != 1
                    or candidate_path_stat.st_nlink != 1
                    or (descriptor_stat.st_dev, descriptor_stat.st_ino)
                    != (candidate_path_stat.st_dev, candidate_path_stat.st_ino)
                ):
                    raise ValueError(
                        f"{field} 候选在发布检查期间发生变化：{anchor.path}"
                    )
                try:
                    replace_descriptor_path(
                        candidate_name,
                        anchor.relative.name,
                        source_dir_fd=anchor.parent_descriptor,
                        destination_dir_fd=anchor.parent_descriptor,
                    )
                except (NotImplementedError, TypeError) as exc:
                    raise RuntimeError(
                        "当前平台不支持安全的描述符相对原子发布。"
                    ) from exc
                published = True
                if fsync_parent:
                    os.fsync(anchor.parent_descriptor)

            yield publish
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            active = False
            cleanup_error: BaseException | None = None
            close_error: BaseException | None = None
            try:
                if not published:
                    candidate_path_stat = stat_descriptor_path(
                        candidate_name,
                        dir_fd=anchor.parent_descriptor,
                    )
                    descriptor_stat = os.fstat(candidate_descriptor)
                    if (
                        stat.S_ISREG(candidate_path_stat.st_mode)
                        and (candidate_path_stat.st_dev, candidate_path_stat.st_ino)
                        == (descriptor_stat.st_dev, descriptor_stat.st_ino)
                    ):
                        unlink_descriptor_path(
                            candidate_name,
                            dir_fd=anchor.parent_descriptor,
                        )
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
            finally:
                try:
                    os.close(candidate_descriptor)
                except BaseException as exc:
                    close_error = exc

            def add_cleanup_note(error: BaseException, note: str) -> None:
                try:
                    add_note = getattr(error, "add_note", None)
                    if callable(add_note):
                        add_note(note)
                except BaseException:
                    pass

            if cleanup_error is not None and close_error is not None:
                add_cleanup_note(
                    cleanup_error,
                    "候选文件描述符关闭还发生了附加错误："
                    f"{type(close_error).__name__}: {close_error}",
                )
            elif cleanup_error is None:
                cleanup_error = close_error

            if cleanup_error is not None:
                if primary_error is not None:
                    add_cleanup_note(
                        primary_error,
                        "候选文件清理还发生了附加错误，未覆盖主异常："
                        f"{type(cleanup_error).__name__}: {cleanup_error}",
                    )
                else:
                    raise cleanup_error


def atomic_write_text(
    output_root: Path,
    stored_path: str | Path,
    content: str,
    *,
    field: str,
    expected_directory: str | None = None,
    expected_leaf: str | None = None,
    fsync_parent: bool = False,
) -> None:
    with staged_atomic_write_text(
        output_root,
        stored_path,
        content,
        field=field,
        expected_directory=expected_directory,
        expected_leaf=expected_leaf,
        fsync_parent=fsync_parent,
    ) as publish:
        publish()


def open_private_text_for_write(
    output_root: Path,
    stored_path: str | Path,
    *,
    directory: str,
    field: str,
    buffering: int = -1,
) -> Any:
    """Open a private output file through anchored descriptors before truncating."""
    relative = validated_output_artifact_relative(
        stored_path,
        directory=directory,
        field=field,
    )
    path = output_root / relative

    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory_only = getattr(os, "O_DIRECTORY", 0)
    non_blocking = getattr(os, "O_NONBLOCK", 0)
    supports_dir_fd = getattr(os, "supports_dir_fd", set())
    supports_follow_symlinks = getattr(os, "supports_follow_symlinks", set())
    required_dir_fd_functions = {os.open, os.mkdir, os.stat}
    if (
        not no_follow
        or not directory_only
        or not non_blocking
        or not required_dir_fd_functions.issubset(supports_dir_fd)
        or os.stat not in supports_follow_symlinks
    ):
        raise RuntimeError("当前平台不支持安全的输出事件描述符写入。")

    common_flags = (
        no_follow
        | non_blocking
        | getattr(os, "O_CLOEXEC", 0)
    )
    directory_flags = os.O_RDONLY | directory_only | common_flags
    leaf_flags = os.O_WRONLY | os.O_CREAT | common_flags
    unsafe_errnos = {
        errno.ELOOP,
        errno.ENOTDIR,
        errno.EISDIR,
        errno.ENXIO,
    }
    unsafe_path_message = f"{field} 路径在描述符写入时不安全：{path}"
    expected_uid = os.geteuid() if hasattr(os, "geteuid") else None

    def validate_directory(
        descriptor: int,
        *,
        label: str,
        normalize_permissions: bool = False,
    ) -> None:
        opened_stat = os.fstat(descriptor)
        if not stat.S_ISDIR(opened_stat.st_mode):
            raise ValueError(f"{field} 的 {label} 不是目录：{path}")
        if expected_uid is not None and opened_stat.st_uid != expected_uid:
            raise ValueError(f"{field} 的 {label} 不属于当前用户：{path}")
        mode = stat.S_IMODE(opened_stat.st_mode)
        if mode & 0o077 or mode & 0o700 != 0o700:
            if not normalize_permissions:
                raise ValueError(
                    f"{field} 的 {label} 权限必须是 0700，"
                    f"当前为 {mode:04o}：{path}"
                )
            os.fchmod(descriptor, 0o700)
            normalized_stat = os.fstat(descriptor)
            normalized_mode = stat.S_IMODE(normalized_stat.st_mode)
            if (
                not stat.S_ISDIR(normalized_stat.st_mode)
                or (
                    expected_uid is not None
                    and normalized_stat.st_uid != expected_uid
                )
                or normalized_mode & 0o077
                or normalized_mode & 0o700 != 0o700
            ):
                raise ValueError(
                    f"{field} 的 {label} 无法收紧为 0700：{path}"
                )

    with ExitStack() as directory_descriptors:
        try:
            current_descriptor = open_descriptor(output_root, directory_flags)
        except OSError as exc:
            if exc.errno in unsafe_errnos:
                raise ValueError(unsafe_path_message) from exc
            raise
        directory_descriptors.callback(os.close, current_descriptor)
        validate_directory(current_descriptor, label="output_dir")

        for component in relative.parts[:-1]:
            try:
                next_descriptor = open_descriptor(
                    component,
                    directory_flags,
                    dir_fd=current_descriptor,
                )
            except FileNotFoundError:
                try:
                    os.mkdir(component, 0o700, dir_fd=current_descriptor)
                except FileExistsError:
                    pass
                try:
                    next_descriptor = open_descriptor(
                        component,
                        directory_flags,
                        dir_fd=current_descriptor,
                    )
                except OSError as exc:
                    if exc.errno in unsafe_errnos:
                        raise ValueError(unsafe_path_message) from exc
                    raise
            except OSError as exc:
                if exc.errno in unsafe_errnos:
                    raise ValueError(unsafe_path_message) from exc
                raise
            directory_descriptors.callback(os.close, next_descriptor)
            validate_directory(
                next_descriptor,
                label="父目录",
                normalize_permissions=True,
            )
            current_descriptor = next_descriptor

        leaf_descriptor: int | None = None
        try:
            try:
                leaf_descriptor = open_descriptor(
                    relative.name,
                    leaf_flags,
                    0o600,
                    dir_fd=current_descriptor,
                )
            except OSError as exc:
                if exc.errno in unsafe_errnos:
                    raise ValueError(unsafe_path_message) from exc
                raise

            opened_stat = os.fstat(leaf_descriptor)
            if not stat.S_ISREG(opened_stat.st_mode):
                raise ValueError(f"{field} 在写入时不是普通文件：{path}")
            if expected_uid is not None and opened_stat.st_uid != expected_uid:
                raise ValueError(f"{field} 在写入时不属于当前用户：{path}")
            if opened_stat.st_nlink != 1:
                raise ValueError(f"{field} 不能存在额外硬链接：{path}")

            try:
                path_stat = stat_descriptor_path(
                    relative.name,
                    dir_fd=current_descriptor,
                )
            except FileNotFoundError as exc:
                raise ValueError(unsafe_path_message) from exc
            if (
                not stat.S_ISREG(path_stat.st_mode)
                or (expected_uid is not None and path_stat.st_uid != expected_uid)
                or path_stat.st_nlink != 1
                or path_stat.st_dev != opened_stat.st_dev
                or path_stat.st_ino != opened_stat.st_ino
            ):
                raise ValueError(
                    f"{field} 路径已在写入检查期间发生变化：{path}"
                )

            final_stat = os.fstat(leaf_descriptor)
            if (
                not stat.S_ISREG(final_stat.st_mode)
                or (expected_uid is not None and final_stat.st_uid != expected_uid)
                or final_stat.st_nlink != 1
                or final_stat.st_dev != opened_stat.st_dev
                or final_stat.st_ino != opened_stat.st_ino
            ):
                raise ValueError(
                    f"{field} 描述符已在写入检查期间发生变化：{path}"
                )

            os.ftruncate(leaf_descriptor, 0)
            os.fchmod(leaf_descriptor, 0o600)
            os.lseek(leaf_descriptor, 0, os.SEEK_SET)
            handle = os.fdopen(
                leaf_descriptor,
                "w",
                encoding="utf-8",
                buffering=buffering,
            )
            leaf_descriptor = None
            return handle
        finally:
            if leaf_descriptor is not None:
                os.close(leaf_descriptor)


def open_output_text_for_read(
    output_root: Path,
    stored_path: str | Path,
    *,
    directory: str,
    field: str,
) -> Any | None:
    """Open an output artifact through anchored descriptors without following links."""
    relative = validated_output_artifact_relative(
        stored_path,
        directory=directory,
        field=field,
    )
    path = output_root / relative

    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory_only = getattr(os, "O_DIRECTORY", 0)
    supports_dir_fd = getattr(os, "supports_dir_fd", set())
    if not no_follow or not directory_only or os.open not in supports_dir_fd:
        raise RuntimeError("当前平台不支持安全的恢复事件描述符读取。")

    common_flags = (
        os.O_RDONLY
        | no_follow
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    directory_flags = common_flags | directory_only
    unsafe_errnos = {errno.ELOOP, errno.ENOTDIR, errno.EISDIR}
    unsafe_path_message = f"{field} 路径在描述符读取时不安全：{path}"

    with ExitStack() as directory_descriptors:
        try:
            current_descriptor = open_descriptor(output_root, directory_flags)
        except OSError as exc:
            if exc.errno in unsafe_errnos:
                raise ValueError(unsafe_path_message) from exc
            raise
        directory_descriptors.callback(os.close, current_descriptor)
        if not stat.S_ISDIR(os.fstat(current_descriptor).st_mode):
            raise ValueError(f"{field} 的 output_dir 不是目录：{output_root}")

        for component in relative.parts[:-1]:
            try:
                next_descriptor = open_descriptor(
                    component,
                    directory_flags,
                    dir_fd=current_descriptor,
                )
            except FileNotFoundError:
                return None
            except OSError as exc:
                if exc.errno in unsafe_errnos:
                    raise ValueError(unsafe_path_message) from exc
                raise
            directory_descriptors.callback(os.close, next_descriptor)
            if not stat.S_ISDIR(os.fstat(next_descriptor).st_mode):
                raise ValueError(f"{field} 父路径在读取时不是目录：{path}")
            current_descriptor = next_descriptor

        leaf_descriptor: int | None = None
        try:
            try:
                leaf_descriptor = open_descriptor(
                    relative.name,
                    common_flags,
                    dir_fd=current_descriptor,
                )
            except FileNotFoundError:
                return None
            except OSError as exc:
                if exc.errno in unsafe_errnos:
                    raise ValueError(unsafe_path_message) from exc
                raise

            opened_stat = os.fstat(leaf_descriptor)
            if not stat.S_ISREG(opened_stat.st_mode):
                raise ValueError(f"{field} 在读取时不是普通文件：{path}")
            if opened_stat.st_nlink != 1:
                raise ValueError(f"{field} 不能存在额外硬链接：{path}")

            handle = os.fdopen(leaf_descriptor, "r", encoding="utf-8")
            leaf_descriptor = None
            return handle
        finally:
            if leaf_descriptor is not None:
                os.close(leaf_descriptor)


def safe_output_artifact_path(
    settings: Settings,
    stored_path: str,
    *,
    directory: str,
    field: str,
) -> Path:
    """Validate artifact components below an already secured output directory."""
    relative = validated_output_artifact_relative(
        stored_path,
        directory=directory,
        field=field,
    )

    output_root = settings.output_dir.resolve()
    candidate = output_root
    last_index = len(relative.parts) - 1
    for index, component in enumerate(relative.parts):
        candidate = candidate / component
        try:
            candidate_stat = os.lstat(candidate)
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(candidate_stat.st_mode):
            raise ValueError(f"{field} 路径不能包含符号链接：{candidate}")
        if index < last_index:
            if not stat.S_ISDIR(candidate_stat.st_mode):
                raise ValueError(f"{field} 父路径必须是普通目录：{candidate}")
        elif not stat.S_ISREG(candidate_stat.st_mode):
            raise ValueError(f"{field} 必须是普通文件路径：{candidate}")
        elif candidate_stat.st_nlink != 1:
            raise ValueError(f"{field} 不能存在额外硬链接：{candidate}")
    return candidate


def ensure_secure_output_directory(settings: Settings) -> None:
    """Create or validate the private host-side control directory."""
    validate_output_location(settings.project_dir, settings.output_dir)
    output_dir = settings.output_dir
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.mkdir(output_dir, 0o700)
    except FileExistsError:
        pass

    output_stat = os.lstat(output_dir)
    if stat.S_ISLNK(output_stat.st_mode) or not stat.S_ISDIR(output_stat.st_mode):
        raise ValueError(f"output_dir 必须是普通目录且不能是符号链接：{output_dir}")
    if hasattr(os, "geteuid") and output_stat.st_uid != os.geteuid():
        raise ValueError(f"output_dir 必须归当前用户所有：{output_dir}")
    mode = stat.S_IMODE(output_stat.st_mode)
    if mode & 0o077 or mode & 0o700 != 0o700:
        raise ValueError(
            f"output_dir 权限必须是 0700，当前为 {mode:04o}：{output_dir}"
        )

    marker_path = output_dir / STATE_MARKER_NAME
    if not os.path.lexists(marker_path):
        existing = list(output_dir.iterdir())
        if existing:
            raise ValueError(
                "output_dir 不是空目录且缺少本工具的状态标记；为防止覆盖无关文件，"
                f"拒绝使用：{output_dir}"
            )
        marker = {
            "application": STATE_MARKER_APPLICATION,
            "schema_version": SCHEMA_VERSION,
            "project_dir": str(settings.project_dir.resolve()),
        }
        atomic_write_text(
            output_dir,
            STATE_MARKER_NAME,
            json.dumps(marker, ensure_ascii=False, sort_keys=True) + "\n",
            field="state_marker",
            expected_leaf=STATE_MARKER_NAME,
            fsync_parent=True,
        )
        return

    marker_stat = os.lstat(marker_path)
    if stat.S_ISLNK(marker_stat.st_mode) or not stat.S_ISREG(marker_stat.st_mode):
        raise ValueError(f"状态标记必须是普通文件且不能是符号链接：{marker_path}")
    if stat.S_IMODE(marker_stat.st_mode) & 0o077:
        raise ValueError(f"状态标记权限不能允许组或其他用户访问：{marker_path}")
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"状态标记无效：{marker_path}") from exc
    expected = {
        "application": STATE_MARKER_APPLICATION,
        "schema_version": SCHEMA_VERSION,
        "project_dir": str(settings.project_dir.resolve()),
    }
    if marker != expected:
        raise ValueError(
            "output_dir 状态标记与当前项目或版本不匹配："
            f"\n状态标记：{marker_path}\n当前项目：{settings.project_dir.resolve()}"
        )


def relative_output_path(settings: Settings, path: Path) -> str:
    return path.relative_to(settings.output_dir.resolve()).as_posix()


def event_record(event: Any) -> dict[str, Any]:
    return {
        "recorded_at": utc_now(),
        "method": str(getattr(event, "method", "")),
        "payload": jsonable(getattr(event, "payload", None)),
    }


def item_type_from_payload(payload: Any) -> str:
    item = nested_get(payload, "item")
    root = unwrap_item(item)
    return str(enum_value(nested_get(root, "type", default="unknown")))


class EventPrinter:
    def __init__(self) -> None:
        self._delta_open = False

    def _line(self, text: str) -> None:
        if self._delta_open:
            print()
            self._delta_open = False
        print(text, flush=True)

    def show(self, event: Any) -> None:
        method = str(getattr(event, "method", ""))
        payload = getattr(event, "payload", None)
        if method == "turn/started":
            self._line("  [Codex] Turn 已开始")
        elif method == "turn/plan/updated":
            self._line("  [Codex] 计划已更新")
        elif method == "turn/diff/updated":
            self._line("  [Codex] 检测到文件差异")
        elif method == "thread/compacted":
            self._line("  [Codex] Thread 上下文已压缩")
        elif method == "item/started":
            item_type = item_type_from_payload(payload)
            labels = {
                "reasoning": "正在分析",
                "commandExecution": "正在运行命令",
                "fileChange": "正在修改文件",
                "mcpToolCall": "正在调用工具",
                "webSearch": "正在检索",
                "plan": "正在制定计划",
            }
            self._line(f"  [Codex] {labels.get(item_type, '开始项目')} ({item_type})")
        elif method == "item/agentMessage/delta":
            delta = nested_get(payload, "delta", default="")
            if isinstance(delta, str) and delta:
                if not self._delta_open:
                    print("  [Codex 消息] ", end="", flush=True)
                    self._delta_open = True
                print(delta, end="", flush=True)
        elif method == "error":
            message = nested_get(payload, "message", default=jsonable(payload))
            self._line(f"  [Codex 错误] {message}")
        elif method == "turn/completed":
            status = enum_value(nested_get(payload, "turn", "status", default="unknown"))
            self._line(f"  [Codex] Turn 已完成，状态：{status}")

    def finish(self) -> None:
        if self._delta_open:
            print()
            self._delta_open = False


def collect_stream(
    handle: Any,
    event_file: Any,
    *,
    show_events: bool,
    expected_turn_id: str,
) -> TurnOutcome:
    items: list[Any] = []
    usage: Any = None
    completed_turn: Any = None
    printer = EventPrinter()

    try:
        for event in handle.stream():
            method = str(getattr(event, "method", ""))
            payload = getattr(event, "payload", None)
            if method == "turn/started":
                matching_turn_id(
                    nested_get(payload, "turn", "id"),
                    expected_turn_id=expected_turn_id,
                    source="turn/started payload.turn.id",
                )
            elif method == "turn/completed":
                matching_turn_id(
                    nested_get(payload, "turn", "id"),
                    expected_turn_id=expected_turn_id,
                    source="turn/completed payload.turn.id",
                )
            event_file.write(
                json.dumps(event_record(event), ensure_ascii=False) + "\n"
            )
            if show_events:
                printer.show(event)
            if method == "item/completed":
                item = nested_get(payload, "item")
                if item is not None:
                    items.append(item)
            elif method == "thread/tokenUsage/updated":
                usage = nested_get(payload, "token_usage", default=None)
                if usage is None:
                    usage = nested_get(payload, "tokenUsage", default=None)
            elif method == "turn/completed":
                completed_turn = nested_get(payload, "turn")
    finally:
        printer.finish()

    if completed_turn is None:
        raise RuntimeError("事件流结束，但没有收到 turn/completed。")

    turn_id = matching_turn_id(
        nested_get(completed_turn, "id"),
        expected_turn_id=expected_turn_id,
        source="turn/completed payload.turn.id",
    )
    status = str(enum_value(nested_get(completed_turn, "status", default="unknown")))
    return TurnOutcome(
        turn_id=turn_id,
        status=status,
        error=nested_get(completed_turn, "error"),
        started_at=nested_get(completed_turn, "started_at"),
        completed_at=nested_get(completed_turn, "completed_at"),
        duration_ms=nested_get(completed_turn, "duration_ms"),
        final_response=final_response_from_items(items),
        items=items,
        usage=usage,
    )


def outcome_from_result(
    result: Any,
    *,
    expected_turn_id: str,
) -> TurnOutcome:
    turn_id = matching_turn_id(
        getattr(result, "id", None),
        expected_turn_id=expected_turn_id,
        source="handle.run() result.id",
    )
    return TurnOutcome(
        turn_id=turn_id,
        status=str(enum_value(result.status)),
        error=result.error,
        started_at=result.started_at,
        completed_at=result.completed_at,
        duration_ms=result.duration_ms,
        final_response=result.final_response,
        items=list(result.items),
        usage=result.usage,
    )


def recover_outcome_from_event_file(
    output_root: Path,
    event_file: str,
    turn_id: str,
) -> TurnOutcome | None:
    try:
        expected_turn_id = required_turn_id(
            turn_id,
            source="stored Attempt turn_id",
        )
    except TurnIdentityError:
        return None

    handle = open_output_text_for_read(
        output_root,
        event_file,
        directory="events",
        field="event_file",
    )
    if handle is None:
        return None
    items: list[Any] = []
    usage: Any = None
    completed_turn: Mapping[str, Any] | None = None
    try:
        with handle:
            for line in handle:
                if not line.strip():
                    continue
                # turn/completed 是可信恢复日志的唯一终点。终态后的任何非空
                # 记录都可能来自拼接或替换，不能继续归入当前 Attempt。
                if completed_turn is not None:
                    return None
                record = json.loads(line)
                if not isinstance(record, Mapping):
                    return None
                method = record.get("method")
                if (
                    not isinstance(method, str)
                    or not method.strip()
                    or method != method.strip()
                ):
                    return None

                if method == "batch/nonStreamingMode":
                    # 非流式模式只保存本地标记，不保存可证明终态的完整事件
                    # 序列；即使文件后来被追加 completed，也不得自动恢复。
                    return None
                if method == "item/completed":
                    payload = record.get("payload")
                    if not isinstance(payload, Mapping):
                        return None
                    item = payload.get("item")
                    if not isinstance(item, Mapping):
                        return None
                    items.append(item)
                elif method == "thread/tokenUsage/updated":
                    # Usage 只是非权威遥测；旧版或未来 SDK 的不同形态不能
                    # 阻止已经通过身份校验的 Turn 终态恢复。
                    payload = record.get("payload")
                    if isinstance(payload, Mapping):
                        usage = payload.get(
                            "tokenUsage",
                            payload.get("token_usage"),
                        )
                elif method == "turn/started":
                    payload = record.get("payload")
                    if not isinstance(payload, Mapping):
                        return None
                    started_turn = payload.get("turn")
                    if not isinstance(started_turn, Mapping):
                        return None
                    try:
                        matching_turn_id(
                            started_turn.get("id"),
                            expected_turn_id=expected_turn_id,
                            source="recovery turn/started payload.turn.id",
                        )
                    except TurnIdentityError:
                        return None
                elif method == "turn/completed":
                    payload = record.get("payload")
                    if not isinstance(payload, Mapping):
                        return None
                    candidate = payload.get("turn")
                    if not isinstance(candidate, Mapping):
                        return None
                    try:
                        matching_turn_id(
                            candidate.get("id"),
                            expected_turn_id=expected_turn_id,
                            source="recovery turn/completed payload.turn.id",
                        )
                    except TurnIdentityError:
                        return None
                    status = candidate.get("status")
                    if (
                        not isinstance(status, str)
                        or not status.strip()
                        or status != status.strip()
                    ):
                        return None
                    completed_turn = candidate
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if completed_turn is None:
        return None
    return TurnOutcome(
        turn_id=expected_turn_id,
        status=completed_turn["status"],
        error=completed_turn.get("error"),
        started_at=completed_turn.get("startedAt", completed_turn.get("started_at")),
        completed_at=completed_turn.get("completedAt", completed_turn.get("completed_at")),
        duration_ms=completed_turn.get("durationMs", completed_turn.get("duration_ms")),
        final_response=final_response_from_items(items),
        items=items,
        usage=usage,
    )


def result_markdown(
    task: TaskSpec,
    *,
    attempt_no: int,
    thread_id: str,
    outcome: TurnOutcome,
    prompt: str,
    agents_snapshot: list[dict[str, Any]],
) -> str:
    agents_lines = "\n".join(
        f"- `{item['path']}` — SHA-256 `{item['sha256']}`"
        for item in agents_snapshot
    )
    if not agents_lines:
        agents_lines = "- 未在项目路径链中发现非空 AGENTS.md/AGENTS.override.md；请检查项目目录。"
    response = outcome.final_response or "（该 Turn 没有生成 final_response。）"
    error = (
        "\n## 错误\n\n```json\n"
        + json.dumps(jsonable(outcome.error), ensure_ascii=False, indent=2)
        + "\n```\n"
        if outcome.error is not None
        else ""
    )
    return f"""# {task.function}

- Task ID：`{task.task_id}`
- Attempt：`{attempt_no}`
- 目标文档：`{task.document}`
- 源文件：`{task.source or '由 Codex 定位'}`
- Thread ID：`{thread_id}`
- Turn ID：`{outcome.turn_id}`
- Turn 状态：`{outcome.status}`
- Duration：`{outcome.duration_ms if outcome.duration_ms is not None else '未知'} ms`
- 保存时间：`{utc_now()}`

## 本轮读取规则快照

{agents_lines}

## 用户任务

````text
{prompt}
````

## Codex final_response

{response}
{error}"""


@contextmanager
def stage_result_file(
    task: TaskSpec,
    *,
    attempt_no: int,
    thread_id: str,
    outcome: TurnOutcome,
    prompt: str,
    agents_snapshot: list[dict[str, Any]],
    output_root: Path,
    response_file: str,
) -> Iterator[Callable[[], None]]:
    with staged_atomic_write_text(
        output_root,
        response_file,
        result_markdown(
            task,
            attempt_no=attempt_no,
            thread_id=thread_id,
            outcome=outcome,
            prompt=prompt,
            agents_snapshot=agents_snapshot,
        ),
        field="response_file",
        expected_directory="responses",
        fsync_parent=True,
    ) as publish:
        yield publish


def render_chat_report(store: Store, settings: Settings) -> None:
    rows = store.all_tasks_with_latest_attempt()
    thread_id = store.get_meta("thread_id") or "尚未创建"
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    summary = " · ".join(
        f"{html.escape(status)} {count}" for status, count in sorted(counts.items())
    )

    cards: list[str] = []
    for row in rows:
        status = str(row["status"])
        prompt = row["prompt"] or (
            f"{row['function_name']} → {row['document']}" if row["enabled"] else "已禁用"
        )
        final_response = row["final_response"]
        error_text = ""
        if row["error_json"]:
            try:
                error_text = json.dumps(
                    json.loads(row["error_json"]), ensure_ascii=False, indent=2
                )
            except json.JSONDecodeError:
                error_text = row["error_json"]
        assistant_text = final_response or error_text or {
            "PENDING": "等待处理",
            "DISABLED": "此任务已禁用",
            "STARTING": "旧版启动状态，无法确认是否已发出 Turn 请求",
            "TURN_NOT_REQUESTED": "尚未发出 Turn 启动请求",
            "TURN_START_REQUESTED": "Turn 启动请求已发出，尚未取得 Turn ID",
            "RUNNING": "Codex 正在处理",
            "INCOMPLETE": "上次运行中断，需要终态证据，禁止自动重试",
            "FAILED": "旧版失败状态，无法确认 Turn 是否已启动",
            "FAILED_BEFORE_REQUEST": "Turn 请求前的本地失败，可以安全重试",
            "FAILED_TERMINAL": "Turn 已返回明确失败终态，可以安全重试",
        }.get(status, status)

        links: list[str] = []
        if row["response_file"]:
            links.append(
                f'<a href="{html.escape(row["response_file"])}">独立 Markdown</a>'
            )
        if row["event_file"]:
            links.append(f'<a href="{html.escape(row["event_file"])}">事件 JSONL</a>')
        link_html = " · ".join(links)
        turn_meta = (
            f"Thread {html.escape(row['thread_id'] or '')} · "
            f"Turn {html.escape(row['turn_id'] or '')}"
            if row["turn_id"]
            else ""
        )
        cards.append(
            f"""
            <section class="turn">
              <div class="turn-head">
                <span>{row['ordinal']:03d} · {html.escape(row['function_name'])}</span>
                <span class="status status-{html.escape(status.lower())}">{html.escape(status)}</span>
              </div>
              <div class="bubble user"><div class="label">任务</div>{html.escape(prompt)}</div>
              <div class="bubble assistant"><div class="label">Codex final_response</div>{html.escape(assistant_text)}</div>
              <div class="meta">{html.escape(turn_meta)} {link_html}</div>
            </section>
            """
        )

    document = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="30">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Codex 函数文档批处理</title>
  <style>
    :root {{ color-scheme: light dark; font-family: Inter, "Segoe UI", sans-serif; }}
    body {{ margin: 0; background: #f4f5f7; color: #202124; }}
    main {{ width: min(1050px, calc(100% - 32px)); margin: 28px auto 80px; }}
    header {{ position: sticky; top: 0; z-index: 2; background: rgba(244,245,247,.94);
              backdrop-filter: blur(10px); padding: 12px 0 18px; }}
    h1 {{ margin: 0 0 8px; font-size: 26px; }}
    .summary, .meta {{ color: #6b7280; font-size: 13px; }}
    .turn {{ border-top: 1px solid #d8dadd; padding: 24px 0 30px; }}
    .turn-head {{ display: flex; justify-content: space-between; gap: 16px;
                  font-weight: 650; margin-bottom: 16px; }}
    .bubble {{ width: min(86%, 880px); white-space: pre-wrap; line-height: 1.65;
               padding: 15px 18px; border-radius: 18px; margin: 12px 0;
               box-shadow: 0 1px 2px rgba(0,0,0,.05); }}
    .user {{ margin-left: auto; background: #e8e9ec; }}
    .assistant {{ background: #fff; border: 1px solid #e2e4e8; }}
    .label {{ color: #777; font-size: 12px; font-weight: 650; margin-bottom: 5px; }}
    .status {{ font-size: 12px; padding: 4px 9px; border-radius: 999px; background: #e5e7eb; }}
    .status-succeeded {{ background: #d9fbe7; color: #12633b; }}
    .status-failed, .status-failed_before_request, .status-failed_terminal,
    .status-incomplete, .status-starting {{
      background: #fee2e2; color: #991b1b;
    }}
    .status-running, .status-turn_not_requested,
    .status-turn_start_requested {{
      background: #dbeafe; color: #1e40af;
    }}
    a {{ color: #1677d2; text-decoration: none; }}
    @media (prefers-color-scheme: dark) {{
      body {{ background: #18191b; color: #e7e7e7; }}
      header {{ background: rgba(24,25,27,.94); }}
      .turn {{ border-color: #34363a; }}
      .user {{ background: #34363a; }}
      .assistant {{ background: #232427; border-color: #3a3c40; }}
    }}
  </style>
</head>
<body>
<main>
  <header>
    <h1>Codex 函数文档批处理</h1>
    <div class="summary">Thread ID：{html.escape(thread_id)} · {summary} · 每 30 秒自动刷新</div>
  </header>
  {''.join(cards) if cards else '<p>还没有任务。</p>'}
</main>
</body>
</html>
"""
    atomic_write_text(
        settings.output_dir,
        "chat_report.html",
        document,
        field="chat_report",
        expected_leaf="chat_report.html",
    )


def recover_completed_attempts(store: Store, settings: Settings) -> None:
    for row in store.unresolved_tasks():
        # 未知或放错表的本地状态必须留给人工对账，不能根据旧事件日志
        # 自动重写为当前协议终态。
        if row["status"] not in KNOWN_TASK_STATUSES:
            continue
        if any(
            attempt_row["status"] not in KNOWN_ATTEMPT_STATUSES
            for attempt_row in store.unresolved_attempts(row["task_id"])
        ):
            continue
        attempt = store.latest_attempt(row["task_id"])
        if (
            not attempt
            or attempt["status"] not in KNOWN_ATTEMPT_STATUSES
            or not attempt["turn_id"]
        ):
            continue
        safe_output_artifact_path(
            settings,
            attempt["event_file"],
            directory="events",
            field="event_file",
        )
        outcome = recover_outcome_from_event_file(
            settings.output_dir,
            attempt["event_file"],
            attempt["turn_id"],
        )
        if outcome is None:
            continue
        result = classify_turn_outcome(outcome)
        has_final = bool(outcome.final_response and outcome.final_response.strip())
        task_status = {
            TaskRunResult.SUCCEEDED: "SUCCEEDED",
            TaskRunResult.FAILED_TERMINAL: "FAILED_TERMINAL",
            TaskRunResult.INCOMPLETE_UNKNOWN: "INCOMPLETE",
        }[result]
        attempt_status = task_status
        task = TaskSpec(
            task_id=row["task_id"],
            ordinal=row["ordinal"],
            enabled=bool(row["enabled"]),
            document=row["document"],
            function=row["function_name"],
            source=row["source"],
            custom_prompt=row["custom_prompt"],
        )
        try:
            snapshot = json.loads(attempt["agents_snapshot_json"])
        except (TypeError, json.JSONDecodeError):
            snapshot = []
        response_path = safe_output_artifact_path(
            settings,
            attempt["response_file"],
            directory="responses",
            field="response_file",
        )
        with stage_result_file(
            task,
            attempt_no=attempt["attempt_no"],
            thread_id=attempt["thread_id"] or store.get_meta("thread_id") or "",
            outcome=outcome,
            prompt=attempt["prompt"],
            agents_snapshot=snapshot,
            output_root=settings.output_dir,
            response_file=attempt["response_file"],
        ) as publish_response:
            store.finish_attempt(
                row["task_id"],
                attempt["attempt_no"],
                expected_task_status=str(row["status"]),
                expected_attempt_status=str(attempt["status"]),
                expected_turn_id=attempt["turn_id"],
                task_status=task_status,
                attempt_status=attempt_status,
                final_response=outcome.final_response,
                error=outcome.error
                if outcome.error is not None
                else (None if has_final else "Turn 已完成，但没有 final_response"),
                usage=outcome.usage,
                duration_ms=outcome.duration_ms,
                publish_response=publish_response,
            )


def load_sdk_api() -> SimpleNamespace:
    try:
        import openai_codex
        from openai_codex import ApprovalMode, Codex, Sandbox
        from openai_codex.types import ReasoningEffort
    except ImportError as exc:
        raise RuntimeError(
            "没有安装 openai-codex。请先执行："
            f"\n{sys.executable} -m pip install -r requirements.txt"
        ) from exc
    return SimpleNamespace(
        module=openai_codex,
        Codex=Codex,
        Sandbox=Sandbox,
        ApprovalMode=ApprovalMode,
        ReasoningEffort=ReasoningEffort,
    )


def effort_value(sdk_api: Any, effort: str | None) -> Any:
    if effort is None:
        return None
    effort_type = getattr(sdk_api, "ReasoningEffort", None)
    if effort_type is None:
        return effort
    try:
        return effort_type(effort)
    except (TypeError, ValueError) as exc:
        valid = ", ".join(str(enum_value(item)) for item in effort_type)
        raise ValueError(f"无效 effort={effort!r}；可用值：{valid}") from exc


def task_cwd(settings: Settings, task: TaskSpec) -> Path:
    # if task.document:
    #     return (settings.project_dir / task.document).parent.resolve()
    # if task.source:
    #     return (settings.project_dir / task.source).parent.resolve()
    return settings.project_dir

def thread_kwargs(settings: Settings, task: TaskSpec, sdk_api: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {# 使用目标文档所在目录，让 Codex 的官方 AGENTS.md 发现链从 Git 根
        # 一直走到目标子目录；Sandbox 仍保持 workspace_write。
        "cwd": str(task_cwd(settings, task)), "sandbox": sdk_api.Sandbox.workspace_write, "approval_mode": sdk_api.ApprovalMode.auto_review, }
    if settings.model:
        kwargs["model"] = settings.model
    return kwargs


def turn_kwargs(
    settings: Settings, task: TaskSpec, sdk_api: Any
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "cwd": str(task_cwd(settings, task)),
        "sandbox": sdk_api.Sandbox.workspace_write,
        "approval_mode": sdk_api.ApprovalMode.auto_review,
    }
    if settings.model:
        kwargs["model"] = settings.model
    if settings.effort:
        kwargs["effort"] = effort_value(sdk_api, settings.effort)
    return kwargs


def run_one_task(
    store: Store,
    settings: Settings,
    task: TaskSpec,
    *,
    sdk_api: Any,
    stream_events: bool,
    force_new_thread: bool = False,
) -> tuple[TaskRunResult, bool]:
    attempt_no = store.next_attempt_no(task.task_id)
    prompt = build_prompt(task, attempt_no)
    snapshot = applicable_agents_snapshot(settings.project_dir, task)
    base_name = attempt_artifact_basename(task.task_id, attempt_no)
    prompt_relative = f"prompts/{base_name}.txt"
    event_relative = f"events/{base_name}.jsonl"
    response_relative = f"responses/{base_name}.md"
    prompt_path = safe_output_artifact_path(
        settings,
        prompt_relative,
        directory="prompts",
        field="prompt_file",
    )
    event_path = safe_output_artifact_path(
        settings,
        event_relative,
        directory="events",
        field="event_file",
    )
    response_path = safe_output_artifact_path(
        settings,
        response_relative,
        directory="responses",
        field="response_file",
    )
    # Prompt 先写入私有候选文件；只有 Attempt 在 SQLite 事务中成功预留三条
    # 工件路径后才发布，路径冲突不会提前覆盖历史 prompt。
    with staged_atomic_write_text(
        settings.output_dir,
        prompt_relative,
        prompt + "\n",
        field="prompt_file",
        expected_directory="prompts",
    ) as publish_prompt:
        store.start_attempt(
            task,
            attempt_no=attempt_no,
            prompt=prompt,
            agents_snapshot=snapshot,
            prompt_file=prompt_relative,
            event_file=event_relative,
            response_file=response_relative,
        )
        publish_prompt()
    render_chat_report(store, settings)

    print(
        f"\n[{task.ordinal:03d}] 开始：{task.function} → "
        f"{task.document or '由 Prompt 确定'}",
        flush=True,
    )
    print(f"  Task ID：{task.task_id}，Attempt：{attempt_no}", flush=True)
    if snapshot:
        print(
            "  AGENTS："
            + ", ".join(f"{item['path']}@{item['sha256'][:10]}" for item in snapshot),
            flush=True,
        )
    else:
        print("  警告：项目路径链中未发现非空 AGENTS.md。", flush=True)

    stored_thread_id = store.get_meta("thread_id")
    thread_id = None if force_new_thread else stored_thread_id
    turn_id: str | None = None
    turn_start_requested = False
    checkpoint_status = "TURN_NOT_REQUESTED"
    started_monotonic = time.monotonic()
    handle: Any = None
    new_thread_adopted = False
    try:
        # 每个函数都新建 Codex()，从而重启本地 app-server；随后恢复同一 Thread。
        with ExitStack() as resources:
            event_file = None
            if stream_events:
                event_file = resources.enter_context(
                    open_private_text_for_write(
                        settings.output_dir,
                        event_relative,
                        directory="events",
                        field="event_file",
                        buffering=1,
                    )
                )
            codex = resources.enter_context(sdk_api.Codex())
            kwargs = thread_kwargs(settings, task, sdk_api)
            if thread_id:
                thread = codex.thread_resume(thread_id, **kwargs)
                if thread.id != thread_id:
                    raise RuntimeError(
                        f"恢复后的 Thread ID 不一致：期望 {thread_id}，得到 {thread.id}"
                    )
            else:
                thread = codex.thread_start(ephemeral=False, **kwargs)
                raw_thread_id = getattr(thread, "id", None)
                new_thread_id = (
                    str(raw_thread_id).strip() if raw_thread_id is not None else ""
                )
                # 旧 ID 一直保留到远端新 Thread 已确认创建；随后用一个短事务
                # 直接替换，并且必须在任何 thread.turn() 请求之前完成。
                store.adopt_thread_id(
                    expected_old_thread_id=stored_thread_id,
                    new_thread_id=new_thread_id,
                )
                thread_id = new_thread_id
                new_thread_adopted = True
                try:
                    thread.set_name(settings.thread_name)
                except Exception as exc:
                    print(f"  警告：设置 Thread 名称失败：{exc}", flush=True)

            options = turn_kwargs(settings, task, sdk_api)
            store.mark_turn_start_requested(
                task.task_id,
                attempt_no,
                thread_id=thread_id,
            )
            checkpoint_status = "TURN_START_REQUESTED"
            turn_start_requested = True
            handle = thread.turn(prompt, **options)
            turn_id = required_turn_id(
                getattr(handle, "id", None),
                source="thread.turn() handle.id",
            )
            store.mark_turn_started(
                task.task_id,
                attempt_no,
                thread_id=thread_id,
                turn_id=turn_id,
            )
            checkpoint_status = "RUNNING"
            render_chat_report(store, settings)
            print(f"  Thread ID：{thread_id}", flush=True)
            print(f"  Turn ID：{turn_id}", flush=True)

            if stream_events:
                if event_file is None:
                    raise RuntimeError("流式事件文件尚未打开。")
                outcome = collect_stream(
                    handle,
                    event_file,
                    show_events=True,
                    expected_turn_id=turn_id,
                )
            else:
                atomic_write_text(
                    settings.output_dir,
                    event_relative,
                    json.dumps(
                        {
                            "recorded_at": utc_now(),
                            "method": "batch/nonStreamingMode",
                            "payload": {"turn_id": turn_id},
                        },
                        ensure_ascii=False,
                    )
                    + "\n",
                    field="event_file",
                    expected_directory="events",
                )
                outcome = outcome_from_result(
                    handle.run(),
                    expected_turn_id=turn_id,
                )
            matching_turn_id(
                outcome.turn_id,
                expected_turn_id=turn_id,
                source="TurnOutcome.turn_id",
            )

    except KeyboardInterrupt:
        if handle is not None:
            try:
                handle.interrupt()
                print("\n  已向当前 Turn 发送中断请求。", flush=True)
            except Exception:
                pass
        store.finish_attempt(
            task.task_id,
            attempt_no,
            expected_task_status=checkpoint_status,
            expected_attempt_status=checkpoint_status,
            expected_turn_id=turn_id,
            task_status="INCOMPLETE",
            attempt_status="INCOMPLETE",
            final_response=None,
            error="用户中断；本地无法证明 Turn 已终止，禁止自动重试。",
            usage=None,
            duration_ms=int((time.monotonic() - started_monotonic) * 1000),
        )
        render_chat_report(store, settings)
        raise
    except sqlite3.Error:
        # 本地状态库异常不是 Turn 结果。保留最后一次成功提交的检查点，
        # 交给 CLI 统一停止批处理，避免二次写库掩盖原始异常。
        raise
    except (ThreadAdoptionConflict, TurnCheckpointConflict):
        # 本地 CAS 冲突表示持久状态已被其他路径改变，不是 Turn 结果。
        # 原事务已回滚；保留冲突时的最后提交状态并立即停止批处理。
        raise
    except Exception as exc:
        state_unknown = turn_start_requested or turn_id is not None
        status = "INCOMPLETE" if state_unknown else "FAILED_BEFORE_REQUEST"
        store.finish_attempt(
            task.task_id,
            attempt_no,
            expected_task_status=checkpoint_status,
            expected_attempt_status=checkpoint_status,
            expected_turn_id=turn_id,
            task_status=status,
            attempt_status=status,
            final_response=None,
            error={"type": type(exc).__name__, "message": str(exc)},
            usage=None,
            duration_ms=int((time.monotonic() - started_monotonic) * 1000),
        )
        render_chat_report(store, settings)
        print(f"  失败：{type(exc).__name__}: {exc}", flush=True)
        failure_result = (
            TaskRunResult.INCOMPLETE_UNKNOWN
            if state_unknown
            else TaskRunResult.FAILED_TERMINAL
        )
        return failure_result, new_thread_adopted

    result = classify_turn_outcome(outcome)
    has_final = bool(outcome.final_response and outcome.final_response.strip())
    task_status = {
        TaskRunResult.SUCCEEDED: "SUCCEEDED",
        TaskRunResult.FAILED_TERMINAL: "FAILED_TERMINAL",
        TaskRunResult.INCOMPLETE_UNKNOWN: "INCOMPLETE",
    }[result]
    # 候选回复先在事务外写入并 fsync；数据库双 CAS 成功后、提交前才原子
    # 发布。CAS 冲突不会创建或覆盖最终回复，发布失败也会回滚数据库更新。
    with stage_result_file(
        task,
        attempt_no=attempt_no,
        thread_id=thread_id or "",
        outcome=outcome,
        prompt=prompt,
        agents_snapshot=snapshot,
        output_root=settings.output_dir,
        response_file=response_relative,
    ) as publish_response:
        store.finish_attempt(
            task.task_id,
            attempt_no,
            expected_task_status="RUNNING",
            expected_attempt_status="RUNNING",
            expected_turn_id=turn_id,
            task_status=task_status,
            attempt_status=task_status,
            final_response=outcome.final_response,
            error=outcome.error,
            usage=outcome.usage,
            duration_ms=outcome.duration_ms,
            publish_response=publish_response,
        )
    render_chat_report(store, settings)

    if result is TaskRunResult.SUCCEEDED:
        print(f"  完成：{response_path}", flush=True)
        return result, new_thread_adopted
    print(
        f"  Turn 状态异常：{outcome.status}；"
        f"final_response={'有' if has_final else '无'}",
        flush=True,
    )
    return result, new_thread_adopted


def status_lines(store: Store) -> list[str]:
    lines: list[str] = []
    for row in store.all_tasks_with_latest_attempt():
        lines.append(
            f"{row['ordinal']:03d}  {row['status']:<10}  "
            f"{row['task_id']}  {row['function_name']}  "
            f"turn={row['turn_id'] or '-'}"
        )
    return lines


def prepare_store(settings: Settings, tasks: Sequence[TaskSpec]) -> Store:
    ensure_secure_output_directory(settings)
    store = Store(settings.output_dir / "run.sqlite3")
    try:
        # project_dir 是状态目录的身份绑定，与输出目录 marker 一致；即使后续
        # 准备失败也保留。任务同步则由自身事务保证全成或全不成。
        store.bind_project(settings.project_dir)
        store.sync_tasks(tasks)
        recover_completed_attempts(store, settings)
        render_chat_report(store, settings)
        # config_path/tasks_file 只描述一次已完成的准备流程，不能提前记录
        # 后续被任务校验、恢复或报告生成拒绝的输入。
        store.set_meta_many(
            {
                "config_path": str(settings.config_path),
                "tasks_file": str(settings.tasks_file),
            }
        )
        return store
    except BaseException:
        try:
            store.close()
        except BaseException:
            pass
        raise


def dry_run_output(settings: Settings, tasks: Sequence[TaskSpec]) -> str:
    enabled = [task for task in tasks if task.enabled]
    lines = [
        f"项目：{settings.project_dir}",
        f"任务文件：{settings.tasks_file}",
        f"输出目录：{settings.output_dir}",
        f"启用任务：{len(enabled)} / {len(tasks)}",
        "",
    ]
    for task in enabled:
        lines.extend(
            [
                f"===== {task.ordinal:03d} {task.task_id} =====",
                build_prompt(task, 1),
                "",
            ]
        )
    return "\n".join(lines)


def execute_batch(
    settings: Settings,
    tasks: Sequence[TaskSpec],
    *,
    limit: int | None = None,
    no_stream: bool = False,
    new_thread: bool = False,
    rerun: Sequence[str] = (),
    retry_incomplete: Sequence[str] = (),
    force_unlock: bool = False,
    sdk_api: Any | None = None,
) -> int:
    ensure_secure_output_directory(settings)
    with RunLock(settings.output_dir / "run.lock", force=force_unlock):
        with prepare_store(settings, tasks) as store:
            unresolved, unresolved_attempts = store.apply_retry_options(
                rerun=rerun,
                retry_incomplete=retry_incomplete,
            )
            if unresolved or unresolved_attempts:
                ids = sorted(
                    {row["task_id"] for row in unresolved}
                    | {row["task_id"] for row in unresolved_attempts}
                )
                task_details = ", ".join(
                    f"{row['task_id']}:{row['status']}" for row in unresolved
                )
                attempt_details = ", ".join(
                    f"{row['task_id']}/a{row['attempt_no']}:{row['status']}"
                    for row in unresolved_attempts
                )
                print(
                    "发现上次未能确认完成状态的任务或 Attempt："
                    f"{', '.join(ids)}"
                    + (f"\n未完成 Task：{task_details}" if task_details else "")
                    + (f"\n未完成 Attempt：{attempt_details}" if attempt_details else "")
                    + "\n禁止使用 --rerun 跳过该状态。\n"
                    "只有 task/attempt 均为 TURN_NOT_REQUESTED 且没有 "
                    "Thread/Turn ID 的"
                    "请求前中断，才可使用：\n"
                    "  python3 run_batch.py --config config.json "
                    "--retry-incomplete <task-id>\n"
                    "旧版 STARTING/FAILED/ABANDONED、TURN_START_REQUESTED、"
                    "RUNNING 和 INCOMPLETE 均不能自动重试，因为本地记录无法证明"
                    "旧 Turn 已终止。任何未识别或表归属错误的状态也按未完成处理。",
                    file=sys.stderr,
                )
                render_chat_report(store, settings)
                return 2

            selected: list[TaskSpec] = []
            for task in tasks:
                if not task.enabled:
                    continue
                row = store.task(task.task_id)
                if row and row["status"] in STARTABLE_TASK_STATUSES:
                    selected.append(task)
            if limit is not None:
                selected = selected[:limit]

            if not selected:
                render_chat_report(store, settings)
                print("没有待处理任务。")
                return 0

            if sdk_api is None:
                sdk_api = load_sdk_api()
            module = getattr(sdk_api, "module", None)
            version = getattr(module, "__version__", None)
            if version:
                store.set_meta("sdk_version", str(version))

            if new_thread:
                print(
                    "将在新 Thread 创建成功后切换；创建失败时保留现有 Thread ID。",
                    flush=True,
                )

            stream_events = settings.stream_events and not no_stream
            failures = 0
            new_thread_pending = new_thread
            for task in selected:
                result, new_thread_adopted = run_one_task(
                    store,
                    settings,
                    task,
                    sdk_api=sdk_api,
                    stream_events=stream_events,
                    force_new_thread=new_thread_pending,
                )
                if new_thread_pending and new_thread_adopted:
                    new_thread_pending = False
                if result is not TaskRunResult.SUCCEEDED:
                    failures += 1
                    if result is TaskRunResult.INCOMPLETE_UNKNOWN:
                        print(
                            "当前 Turn 状态未知或未完成；为避免并发或重复修改，"
                            "无论 continue_on_error 如何设置都停止后续任务。",
                            flush=True,
                        )
                        break
                    if not settings.continue_on_error:
                        print("已按 continue_on_error=false 停止后续任务。", flush=True)
                        break

            print("\n批处理状态：")
            for line in status_lines(store):
                print(line)
            print(f"\n永久聊天报告：{settings.output_dir / 'chat_report.html'}")
            return 1 if failures else 0
