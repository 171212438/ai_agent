from __future__ import annotations

import csv
import contextlib
import errno
import io
import json
import os
import sqlite3
import stat
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from codex_batch import (
    RunLock,
    SCHEMA_VERSION,
    Settings,
    Store,
    TaskSpec,
    ThreadAdoptionConflict,
    TurnCheckpointConflict,
    TurnIdentityError,
    atomic_write_text,
    attempt_artifact_basename,
    build_prompt,
    ensure_secure_output_directory,
    execute_batch,
    final_response_from_items,
    is_sqlite_lock_error,
    load_settings,
    load_tasks,
    open_descriptor,
    open_output_text_for_read,
    open_private_text_for_write,
    prepare_store,
    recover_completed_attempts,
    relative_output_path,
    run_one_task,
    safe_output_artifact_path,
    staged_atomic_write_text,
    stat_descriptor_path,
)
from run_batch import main as run_batch_main


class FakeStatus:
    def __init__(self, value: str) -> None:
        self.value = value


class FakeWorld:
    def __init__(self) -> None:
        self.codex_instances = 0
        self.start_count = 0
        self.thread_start_error_starts: set[int] = set()
        self.thread_start_ids: dict[int, str] = {}
        self.resume_ids: list[str] = []
        self.turn_ids: list[str] = []
        self.turn_request_count = 0
        self.remote_accept_count = 0
        self.stream_count = 0
        self.run_count = 0
        self.lost_turn_start_responses: set[int] = set()
        self.stream_error_turns: set[int] = set()
        self.terminal_failure_turns: set[int] = set()
        self.unknown_status_turns: set[int] = set()
        self.handle_turn_id_overrides: dict[int, Any] = {}
        self.started_turn_id_overrides: dict[int, Any] = {}
        self.completed_turn_id_overrides: dict[int, Any] = {}
        self.completed_turn_id_omissions: set[int] = set()
        self.run_result_turn_id_overrides: dict[int, Any] = {}


class FakeTurnHandle:
    def __init__(self, world: FakeWorld, prompt: str) -> None:
        self.world = world
        self.prompt = prompt
        self.turn_number = len(world.turn_ids) + 1
        self.remote_id = f"turn-{self.turn_number}"
        world.turn_ids.append(self.remote_id)
        self.id = world.handle_turn_id_overrides.get(
            self.turn_number,
            self.remote_id,
        )

    def stream(self):
        self.world.stream_count += 1
        yield SimpleNamespace(
            method="turn/started",
            payload=SimpleNamespace(
                turn=SimpleNamespace(
                    id=self.world.started_turn_id_overrides.get(
                        self.turn_number,
                        self.id,
                    )
                )
            ),
        )
        if self.turn_number in self.world.stream_error_turns:
            raise RuntimeError("模拟事件流在 Turn 启动后中断")
        if self.turn_number in self.world.terminal_failure_turns:
            yield SimpleNamespace(
                method="turn/completed",
                payload=SimpleNamespace(
                    turn=SimpleNamespace(
                        id=self.world.completed_turn_id_overrides.get(
                            self.turn_number,
                            self.id,
                        ),
                        status=FakeStatus("failed"),
                        error=SimpleNamespace(message="模拟明确终态失败"),
                        started_at=100,
                        completed_at=101,
                        duration_ms=1000,
                    )
                ),
            )
            return
        message = SimpleNamespace(
            type="agentMessage",
            phase="finalAnswer",
            text=f"已完成 {self.id}",
        )
        yield SimpleNamespace(
            method="item/completed",
            payload=SimpleNamespace(item=SimpleNamespace(root=message)),
        )
        completed_turn = {
            "status": FakeStatus(
                "future-unknown"
                if self.turn_number in self.world.unknown_status_turns
                else "completed"
            ),
            "error": None,
            "started_at": 100,
            "completed_at": 101,
            "duration_ms": 1000,
        }
        if self.turn_number not in self.world.completed_turn_id_omissions:
            completed_turn["id"] = self.world.completed_turn_id_overrides.get(
                self.turn_number,
                self.id,
            )
        yield SimpleNamespace(
            method="turn/completed",
            payload=SimpleNamespace(turn=SimpleNamespace(**completed_turn)),
        )

    def run(self):
        self.world.run_count += 1
        terminal_failure = self.turn_number in self.world.terminal_failure_turns
        unknown_status = self.turn_number in self.world.unknown_status_turns
        return SimpleNamespace(
            id=self.world.run_result_turn_id_overrides.get(
                self.turn_number,
                self.id,
            ),
            status=FakeStatus(
                "failed"
                if terminal_failure
                else ("future-unknown" if unknown_status else "completed")
            ),
            error=(
                SimpleNamespace(message="模拟明确终态失败")
                if terminal_failure
                else None
            ),
            started_at=100,
            completed_at=101,
            duration_ms=1000,
            final_response=(
                None if terminal_failure else f"已完成 {self.id}"
            ),
            items=[],
            usage=None,
        )

    def interrupt(self) -> None:
        return None


class FakeThread:
    def __init__(self, world: FakeWorld, thread_id: str) -> None:
        self.world = world
        self.id = thread_id
        self.name = ""

    def set_name(self, name: str) -> None:
        self.name = name

    def turn(self, prompt: str, **_kwargs):
        self.world.turn_request_count += 1
        self.world.remote_accept_count += 1
        if self.world.turn_request_count in self.world.lost_turn_start_responses:
            raise RuntimeError("模拟服务端已受理 Turn，但启动响应丢失")
        return FakeTurnHandle(self.world, prompt)


class FakeCodex:
    world: FakeWorld

    def __init__(self) -> None:
        self.world.codex_instances += 1

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        return None

    def thread_start(self, **_kwargs):
        self.world.start_count += 1
        if self.world.start_count in self.world.thread_start_error_starts:
            raise RuntimeError("模拟 Turn 请求前的 Thread 启动失败")
        thread_id = self.world.thread_start_ids.get(
            self.world.start_count,
            "thread-shared",
        )
        return FakeThread(self.world, thread_id)

    def thread_resume(self, thread_id: str, **_kwargs):
        self.world.resume_ids.append(thread_id)
        return FakeThread(self.world, thread_id)


def fake_sdk(world: FakeWorld):
    FakeCodex.world = world
    return SimpleNamespace(
        module=SimpleNamespace(__version__="test"),
        Codex=FakeCodex,
        Sandbox=SimpleNamespace(workspace_write="workspace-write"),
        ApprovalMode=SimpleNamespace(auto_review="auto-review"),
        ReasoningEffort=None,
    )


class CodexBatchTests(unittest.TestCase):
    @staticmethod
    def _attempt_artifact_relatives(
        task_id: str,
        attempt_no: int,
    ) -> tuple[str, str, str]:
        base_name = attempt_artifact_basename(task_id, attempt_no)
        return (
            f"prompts/{base_name}.txt",
            f"events/{base_name}.jsonl",
            f"responses/{base_name}.md",
        )

    @staticmethod
    def _turn_identity_fixture(
        root: Path,
        *,
        thread_name: str,
    ) -> tuple[Settings, list[TaskSpec]]:
        project_dir = root / "project"
        project_dir.mkdir()
        (project_dir / "AGENTS.md").write_text(
            "# Rules\n",
            encoding="utf-8",
        )
        settings = Settings(
            config_path=root / "config.json",
            project_dir=project_dir,
            tasks_file=root / "tasks.csv",
            output_dir=root / "state",
            thread_name=thread_name,
            model=None,
            effort=None,
            stream_events=True,
            continue_on_error=True,
        )
        tasks = [
            TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
            TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
        ]
        return settings, tasks

    @staticmethod
    def _recovery_fixture(
        root: Path,
        *,
        turn_id: str = "turn-expected",
    ) -> tuple[Settings, TaskSpec, Path, Path]:
        project_dir = root / "project"
        project_dir.mkdir()
        (project_dir / "AGENTS.md").write_text(
            "# Rules\n",
            encoding="utf-8",
        )
        output_dir = root / "state"
        settings = Settings(
            config_path=root / "config.json",
            project_dir=project_dir,
            tasks_file=root / "tasks.csv",
            output_dir=output_dir,
            thread_name="恢复事件 schema 测试",
            model=None,
            effort=None,
            stream_events=True,
            continue_on_error=False,
        )
        task = TaskSpec(
            "recover-schema",
            1,
            True,
            "A.md",
            "A()",
            "A.c",
        )
        event_relative = "events/001-recover-schema-a1.jsonl"
        response_relative = "responses/001-recover-schema-a1.md"
        ensure_secure_output_directory(settings)
        with Store(output_dir / "run.sqlite3") as store:
            store.sync_tasks([task])
            store.set_meta("project_dir", str(project_dir))
            store.set_meta("thread_id", "thread-shared")
            store.start_attempt(
                task,
                attempt_no=1,
                prompt=build_prompt(task, 1),
                agents_snapshot=[],
                prompt_file="prompts/001-recover-schema-a1.txt",
                event_file=event_relative,
                response_file=response_relative,
            )
            store.mark_turn_start_requested(
                task.task_id,
                1,
                thread_id="thread-shared",
            )
            store.mark_turn_started(
                task.task_id,
                1,
                thread_id="thread-shared",
                turn_id=turn_id,
            )

        event_path = output_dir / event_relative
        event_path.parent.mkdir(parents=True)
        response_path = output_dir / response_relative
        response_path.parent.mkdir(parents=True)
        return settings, task, event_path, response_path

    def _assert_recovery_rejected_without_state_change(
        self,
        settings: Settings,
        task: TaskSpec,
        response_path: Path,
        *,
        turn_id: str,
        response_inode: int,
    ) -> None:
        with prepare_store(settings, [task]) as store:
            task_row = store.task(task.task_id)
            attempt = store.latest_attempt(task.task_id)
            self.assertEqual(task_row["status"], "RUNNING")
            self.assertEqual(task_row["latest_attempt"], 1)
            self.assertEqual(attempt["status"], "RUNNING")
            self.assertEqual(attempt["turn_id"], turn_id)
            self.assertIsNone(attempt["completed_at"])
            self.assertIsNone(attempt["duration_ms"])
            self.assertIsNone(attempt["final_response"])
            self.assertIsNone(attempt["error_json"])
            self.assertIsNone(attempt["usage_json"])
        self.assertEqual(response_path.read_text(encoding="utf-8"), "可信旧响应\n")
        self.assertEqual(response_path.stat().st_ino, response_inode)
        self.assertEqual(
            [
                path.name
                for path in response_path.parent.iterdir()
                if path.name.startswith(".codex-batch-")
            ],
            [],
        )

    def test_final_response_prefers_final_answer(self) -> None:
        items = [
            SimpleNamespace(
                root=SimpleNamespace(
                    type="agentMessage", phase=None, text="中间消息"
                )
            ),
            SimpleNamespace(
                root=SimpleNamespace(
                    type="agentMessage", phase="finalAnswer", text="最终消息"
                )
            ),
        ]
        self.assertEqual(final_response_from_items(items), "最终消息")

    def test_prompt_is_one_function_and_forces_agents_reload(self) -> None:
        task = TaskSpec(
            task_id="pwm-init",
            ordinal=1,
            enabled=True,
            document="Bsp_Pwm.md",
            function="Bsp_Pwm_Init()",
            source="Bsp_Pwm.c",
        )
        prompt = build_prompt(task, 1)
        self.assertIn("重新读取", prompt)
        self.assertIn("AGENTS.md", prompt)
        self.assertIn("只处理下面这一个函数", prompt)
        self.assertIn("Bsp_Pwm_Init()", prompt)
        self.assertIn("不得重复插入", prompt)

    def test_run_lock_force_cannot_override_active_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "run.lock"
            with RunLock(lock_path):
                original = lock_path.read_bytes()
                original_inode = lock_path.stat().st_ino
                with self.assertRaisesRegex(RuntimeError, "不会覆盖"):
                    with RunLock(lock_path, force=True):
                        self.fail("活跃运行锁不得被强制覆盖")
                self.assertEqual(lock_path.read_bytes(), original)
                self.assertEqual(lock_path.stat().st_ino, original_inode)

            first_record = json.loads(lock_path.read_text(encoding="utf-8"))
            self.assertEqual(first_record["protocol"], 2)
            with RunLock(lock_path):
                second_record = json.loads(lock_path.read_text(encoding="utf-8"))
                self.assertNotEqual(
                    second_record["owner_token"], first_record["owner_token"]
                )
            self.assertTrue(lock_path.is_file())

    @unittest.skipUnless(hasattr(os, "fork"), "需要 POSIX fork 验证跨进程 flock")
    def test_run_lock_blocks_force_from_another_process(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "run.lock"
            ready_read, ready_write = os.pipe()
            release_read, release_write = os.pipe()
            child_pid = os.fork()
            if child_pid == 0:
                os.close(ready_read)
                os.close(release_write)
                exit_code = 0
                try:
                    with RunLock(lock_path):
                        os.write(ready_write, b"R")
                        if os.read(release_read, 1) != b"X":
                            exit_code = 2
                except BaseException:
                    try:
                        os.write(ready_write, b"E")
                    except OSError:
                        pass
                    exit_code = 3
                finally:
                    os.close(ready_write)
                    os.close(release_read)
                os._exit(exit_code)

            os.close(ready_write)
            os.close(release_read)
            child_status: int | None = None
            try:
                self.assertEqual(os.read(ready_read, 1), b"R")
                original = lock_path.read_bytes()
                with self.assertRaisesRegex(RuntimeError, "不会覆盖"):
                    with RunLock(lock_path, force=True):
                        self.fail("另一个进程持有的锁不得被 force 覆盖")
                self.assertEqual(lock_path.read_bytes(), original)
            finally:
                os.close(ready_read)
                try:
                    os.write(release_write, b"X")
                except BrokenPipeError:
                    pass
                os.close(release_write)
                _waited_pid, child_status = os.waitpid(child_pid, 0)

            self.assertEqual(os.waitstatus_to_exitcode(child_status), 0)
            with RunLock(lock_path):
                self.assertEqual(
                    json.loads(lock_path.read_text(encoding="utf-8"))["protocol"],
                    2,
                )

    def test_force_unlock_refuses_live_legacy_pid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "run.lock"
            original = json.dumps(
                {"pid": os.getpid(), "started_at": "legacy"}
            ).encode("utf-8")
            lock_path.write_bytes(original)
            original_inode = lock_path.stat().st_ino

            with self.assertRaisesRegex(RuntimeError, "仍在运行"):
                with RunLock(lock_path, force=True):
                    self.fail("存活的旧版 PID 不得被强制迁移")

            self.assertEqual(lock_path.read_bytes(), original)
            self.assertEqual(lock_path.stat().st_ino, original_inode)

    def test_force_unlock_migrates_only_proven_stale_legacy_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "run.lock"
            lock_path.write_text(
                json.dumps({"pid": 424242, "started_at": "legacy"}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "旧版运行锁"):
                with RunLock(lock_path):
                    self.fail("旧版锁必须显式迁移")

            with mock.patch(
                "codex_batch.os.kill", side_effect=ProcessLookupError
            ) as kill:
                with RunLock(lock_path, force=True):
                    record = json.loads(lock_path.read_text(encoding="utf-8"))
                    self.assertEqual(record["protocol"], 2)
                    self.assertEqual(record["pid"], os.getpid())
                    self.assertTrue(record["owner_token"])
            kill.assert_called_once_with(424242, 0)

            # v2 锁由内核锁判断是否空闲，正常退出后无需再次 force。
            with RunLock(lock_path):
                self.assertEqual(
                    json.loads(lock_path.read_text(encoding="utf-8"))["protocol"],
                    2,
                )

    def test_force_unlock_rejects_unverifiable_or_invalid_legacy_lock(self) -> None:
        invalid_payloads = [
            b"not-json",
            b"[]",
            b"{}",
            b'{"pid": true}',
            b'{"pid": "123"}',
            b'{"pid": 0}',
            b'{"pid": -1}',
            b'{"pid": 999999999999999999999999999999}',
        ]
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "run.lock"
            for payload in invalid_payloads:
                with self.subTest(payload=payload):
                    lock_path.write_bytes(payload)
                    with mock.patch("codex_batch.os.kill") as kill:
                        with self.assertRaises(RuntimeError):
                            with RunLock(lock_path, force=True):
                                self.fail("无法验证的旧版锁不得被覆盖")
                    kill.assert_not_called()
                    self.assertEqual(lock_path.read_bytes(), payload)

            permission_payload = json.dumps({"pid": 515151}).encode("utf-8")
            lock_path.write_bytes(permission_payload)
            with mock.patch(
                "codex_batch.os.kill", side_effect=PermissionError
            ):
                with self.assertRaisesRegex(RuntimeError, "无法确认"):
                    with RunLock(lock_path, force=True):
                        self.fail("权限不明的 PID 不得被视为已退出")
            self.assertEqual(lock_path.read_bytes(), permission_payload)

    def test_force_unlock_detects_replacement_during_pid_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lock_path = root / "run.lock"
            replacement_path = root / "replacement.lock"
            lock_path.write_text(json.dumps({"pid": 616161}), encoding="utf-8")
            replacement = json.dumps(
                {"pid": os.getpid(), "started_at": "replacement"}
            ).encode("utf-8")

            def check_pid(pid: int, signal: int) -> None:
                self.assertEqual(signal, 0)
                if pid == 616161:
                    replacement_path.write_bytes(replacement)
                    os.replace(replacement_path, lock_path)
                    raise ProcessLookupError
                self.assertEqual(pid, os.getpid())

            with mock.patch("codex_batch.os.kill", side_effect=check_pid):
                with self.assertRaisesRegex(RuntimeError, "仍在运行"):
                    with RunLock(lock_path, force=True):
                        self.fail("PID 检查期间替换的锁不得被旧检查结果覆盖")

            self.assertEqual(lock_path.read_bytes(), replacement)

    def test_status_force_unlock_migrates_stale_lock_without_running_batch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            tasks_path = root / "tasks.csv"
            tasks_path.write_text(
                "id,enabled,document,function,source,prompt\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "project_dir": str(project_dir),
                        "tasks_file": str(tasks_path),
                        "output_dir": str(output_dir),
                    }
                ),
                encoding="utf-8",
            )
            ensure_secure_output_directory(load_settings(config_path))
            lock_path = output_dir / "run.lock"
            legacy = json.dumps(
                {"pid": 717171, "started_at": "legacy"}
            ).encode("utf-8")
            lock_path.write_bytes(legacy)

            without_force_stderr = io.StringIO()
            with contextlib.redirect_stderr(without_force_stderr):
                without_force = run_batch_main(
                    ["--config", str(config_path), "--status"]
                )
            self.assertEqual(without_force, 2)
            self.assertIn("旧版运行锁", without_force_stderr.getvalue())
            self.assertEqual(lock_path.read_bytes(), legacy)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch(
                "codex_batch.os.kill", side_effect=ProcessLookupError
            ) as kill, mock.patch("run_batch.execute_batch") as execute_batch_mock:
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(
                    stderr
                ):
                    result = run_batch_main(
                        [
                            "--config",
                            str(config_path),
                            "--status",
                            "--force-unlock",
                        ]
                    )

            self.assertEqual(result, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertIn("Thread ID：", stdout.getvalue())
            kill.assert_called_once_with(717171, 0)
            execute_batch_mock.assert_not_called()
            migrated = json.loads(lock_path.read_text(encoding="utf-8"))
            self.assertEqual(migrated["protocol"], 2)
            self.assertEqual(migrated["pid"], os.getpid())
            self.assertTrue(migrated["owner_token"])

    def test_cli_modes_reject_ignored_options_before_loading_config(self) -> None:
        invalid_combinations = [
            ("--dry-run", "--status", ["--dry-run", "--status"]),
            ("--dry-run", "--limit", ["--dry-run", "--limit", "1"]),
            ("--dry-run", "--no-stream", ["--dry-run", "--no-stream"]),
            ("--dry-run", "--new-thread", ["--dry-run", "--new-thread"]),
            ("--dry-run", "--rerun", ["--dry-run", "--rerun", "task-a"]),
            (
                "--dry-run",
                "--retry-incomplete",
                ["--dry-run", "--retry-incomplete", "task-a"],
            ),
            ("--dry-run", "--force-unlock", ["--dry-run", "--force-unlock"]),
            ("--status", "--limit", ["--status", "--limit", "1"]),
            ("--status", "--no-stream", ["--status", "--no-stream"]),
            ("--status", "--new-thread", ["--status", "--new-thread"]),
            ("--status", "--rerun", ["--status", "--rerun", "task-a"]),
            (
                "--status",
                "--retry-incomplete",
                ["--status", "--retry-incomplete", "task-a"],
            ),
        ]

        for mode, conflict, argv in invalid_combinations:
            with self.subTest(argv=argv):
                stderr = io.StringIO()
                with mock.patch(
                    "run_batch.load_settings",
                    side_effect=AssertionError("非法组合不得读取配置"),
                ) as load_settings_mock, mock.patch(
                    "run_batch.load_tasks"
                ) as load_tasks_mock, mock.patch(
                    "run_batch.execute_batch"
                ) as execute_batch_mock:
                    with contextlib.redirect_stderr(stderr):
                        with self.assertRaises(SystemExit) as raised:
                            run_batch_main(argv)

                self.assertEqual(raised.exception.code, 2)
                self.assertIn(mode, stderr.getvalue())
                self.assertIn(conflict, stderr.getvalue())
                load_settings_mock.assert_not_called()
                load_tasks_mock.assert_not_called()
                execute_batch_mock.assert_not_called()

    def test_cli_formats_sqlite_lock_without_traceback(self) -> None:
        for mode_args in (("--status",), ()):
            with self.subTest(mode_args=mode_args), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                tasks_path = root / "tasks.csv"
                tasks_path.write_text(
                    "id,enabled,document,function,source,prompt\n"
                    "task-a,true,Doc.md,A(),,test prompt\n",
                    encoding="utf-8",
                )
                config_path = root / "config.json"
                config_path.write_text(
                    json.dumps(
                        {
                            "project_dir": str(project_dir),
                            "tasks_file": str(tasks_path),
                            "output_dir": str(root / "state"),
                        }
                    ),
                    encoding="utf-8",
                )
                lock_error = sqlite3.OperationalError("database is locked")
                lock_error.sqlite_errorcode = 5
                stdout = io.StringIO()
                stderr = io.StringIO()

                with mock.patch.object(
                    Store,
                    "_ensure_wal_mode",
                    side_effect=lock_error,
                ), mock.patch(
                    "codex_batch.load_sdk_api"
                ) as load_sdk_api, mock.patch(
                    "codex_batch.run_one_task"
                ) as run_one_task, contextlib.redirect_stdout(
                    stdout
                ), contextlib.redirect_stderr(
                    stderr
                ):
                    result = run_batch_main(
                        ["--config", str(config_path), *mode_args]
                    )

                self.assertEqual(result, 2)
                self.assertEqual(stdout.getvalue(), "")
                self.assertEqual(
                    stderr.getvalue(),
                    "错误：SQLite 状态库被锁定：database is locked\n",
                )
                self.assertNotIn("Traceback", stderr.getvalue())
                load_sdk_api.assert_not_called()
                run_one_task.assert_not_called()

    def test_cli_does_not_hide_non_lock_sqlite_errors(self) -> None:
        failures: list[sqlite3.Error] = [
            sqlite3.OperationalError("disk I/O error"),
            sqlite3.ProgrammingError("injected SQL programming error"),
            sqlite3.DatabaseError("database disk image is malformed"),
        ]
        failures[0].sqlite_errorcode = 10

        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                settings = SimpleNamespace(
                    tasks_file=Path("unused-tasks.csv"),
                    project_dir=Path("unused-project"),
                )
                stderr = io.StringIO()
                with mock.patch(
                    "run_batch.load_settings",
                    return_value=settings,
                ), mock.patch(
                    "run_batch.load_tasks",
                    return_value=[],
                ), mock.patch(
                    "run_batch.execute_batch",
                    side_effect=failure,
                ), contextlib.redirect_stderr(
                    stderr
                ):
                    with self.assertRaises(type(failure)) as raised:
                        run_batch_main([])

                self.assertIs(raised.exception, failure)
                self.assertEqual(stderr.getvalue(), "")

    def test_cli_stops_batch_after_run_one_task_sqlite_checkpoint_error(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            tasks_path = root / "tasks.csv"
            tasks_path.write_text(
                "id,enabled,document,function,source,prompt\n"
                "task-a,true,A.md,A(),,test A\n"
                "task-b,true,B.md,B(),,test B\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "project_dir": str(project_dir),
                        "tasks_file": str(tasks_path),
                        "output_dir": str(output_dir),
                        "continue_on_error": True,
                    }
                ),
                encoding="utf-8",
            )
            world = FakeWorld()
            lock_error = sqlite3.OperationalError("database is locked")
            lock_error.sqlite_errorcode = 5
            real_mark_turn_start_requested = Store.mark_turn_start_requested

            def fail_first_checkpoint(
                store,
                task_id,
                attempt_no,
                *,
                thread_id,
            ):
                if task_id == "task-a":
                    raise lock_error
                return real_mark_turn_start_requested(
                    store,
                    task_id,
                    attempt_no,
                    thread_id=thread_id,
                )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(
                Store,
                "mark_turn_start_requested",
                new=fail_first_checkpoint,
            ), mock.patch(
                "codex_batch.load_sdk_api",
                return_value=fake_sdk(world),
            ), contextlib.redirect_stdout(
                stdout
            ), contextlib.redirect_stderr(
                stderr
            ):
                result = run_batch_main(["--config", str(config_path)])

            self.assertEqual(result, 2)
            self.assertEqual(
                stderr.getvalue(),
                "错误：SQLite 状态库被锁定：database is locked\n",
            )
            self.assertNotIn("[002]", stdout.getvalue())
            self.assertEqual(world.start_count, 1)
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)

            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(
                    store.task("task-a")["status"],
                    "TURN_NOT_REQUESTED",
                )
                self.assertEqual(
                    store.latest_attempt("task-a")["status"],
                    "TURN_NOT_REQUESTED",
                )
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

    def test_run_lock_exit_preserves_replacement_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lock_path = root / "run.lock"
            replacement_path = root / "replacement.lock"
            replacement = b'{"protocol": 2, "replacement": true}'

            with RunLock(lock_path):
                replacement_path.write_bytes(replacement)
                os.replace(replacement_path, lock_path)

            self.assertEqual(lock_path.read_bytes(), replacement)

    def test_load_csv_rejects_duplicate_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tasks_path = root / "tasks.csv"
            with tasks_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["id", "enabled", "document", "function", "source", "prompt"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "id": "same",
                        "enabled": "true",
                        "document": "A.md",
                        "function": "A()",
                    }
                )
                writer.writerow(
                    {
                        "id": "same",
                        "enabled": "true",
                        "document": "B.md",
                        "function": "B()",
                    }
                )
            with self.assertRaisesRegex(ValueError, "重复的任务 id"):
                load_tasks(tasks_path, root)

    def test_load_csv_rejects_invalid_header_or_row_shape(self) -> None:
        cases = (
            ("empty-file", "", "缺少表头"),
            (
                "unknown-header",
                "id,enabeld,document,function,source,prompt\n"
                "task-a,false,A.md,A(),,\n",
                "未知字段.*enabeld",
            ),
            (
                "duplicate-header",
                "id,enabled,enabled,document,function,source,prompt\n"
                "task-a,true,false,A.md,A(),,\n",
                "重复字段.*enabled",
            ),
            (
                "blank-header",
                "id,,document,function,source,prompt\n"
                "task-a,false,A.md,A(),,\n",
                "空字段名",
            ),
            (
                "missing-required-header",
                "id,enabled,document,source,prompt\n",
                "缺少必需字段.*function",
            ),
            (
                "missing-target-header",
                "id,function,enabled\n",
                "document.*target.*prompt.*custom_prompt",
            ),
            (
                "conflicting-document-alias",
                "id,function,document,target\n"
                "task-a,A(),A.md,B.md\n",
                "不能同时包含.*document.*target",
            ),
            (
                "conflicting-prompt-alias",
                "id,function,prompt,custom_prompt\n"
                "task-a,A(),first,second\n",
                "不能同时包含.*prompt.*custom_prompt",
            ),
            (
                "short-row",
                "id,enabled,document,function,source,prompt\n"
                "task-a\n",
                "第 2 行缺少列",
            ),
            (
                "long-row",
                "id,enabled,document,function,source,prompt\n"
                "task-a,false,A.md,A(),,,unexpected\n",
                "第 2 行包含多余列",
            ),
            (
                "malformed-quote",
                "id,enabled,document,function,source,prompt\n"
                '"task-a,false,A.md,A(),,\n',
                "任务 CSV 无效",
            ),
        )
        for case, content, message in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                tasks_path = root / "tasks.csv"
                tasks_path.write_text(content, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, message):
                    load_tasks(tasks_path, root)

    def test_load_csv_accepts_known_reordered_and_legacy_headers(self) -> None:
        cases = (
            (
                "reordered-canonical",
                "function,id,document,enabled\n"
                "A(),task-a,A.md,false\n",
                False,
                "A.md",
            ),
            (
                "legacy-aliases",
                "custom_prompt,target,function,id\n"
                ",B.md,B(),task-b\n",
                True,
                "B.md",
            ),
        )
        for case, content, enabled, document in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                tasks_path = root / "tasks.csv"
                tasks_path.write_text(content, encoding="utf-8")
                tasks = load_tasks(tasks_path, root)
                self.assertEqual(len(tasks), 1)
                self.assertEqual(tasks[0].enabled, enabled)
                self.assertEqual(tasks[0].document, document)
                self.assertEqual(tasks[0].source, "")
                self.assertEqual(tasks[0].custom_prompt, "")

    def test_cli_rejects_unknown_csv_header_before_creating_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            tasks_path = root / "tasks.csv"
            tasks_path.write_text(
                "id,enabeld,document,function,source,prompt\n"
                "task-a,false,A.md,A(),,\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "project_dir": str(project_dir),
                        "tasks_file": str(tasks_path),
                        "output_dir": str(output_dir),
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with mock.patch(
                "run_batch.execute_batch"
            ) as execute_batch_mock, mock.patch(
                "codex_batch.load_sdk_api"
            ) as load_sdk_api, contextlib.redirect_stdout(
                stdout
            ), contextlib.redirect_stderr(
                stderr
            ):
                result = run_batch_main(["--config", str(config_path)])

            self.assertEqual(result, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("未知字段", stderr.getvalue())
            self.assertIn("enabeld", stderr.getvalue())
            self.assertFalse(output_dir.exists())
            execute_batch_mock.assert_not_called()
            load_sdk_api.assert_not_called()

    def test_load_json_rejects_unknown_conflicting_or_invalid_fields(
        self,
    ) -> None:
        base = {
            "id": "task-a",
            "function": "A()",
            "document": "A.md",
        }

        def task_json(**changes: Any) -> str:
            return json.dumps(
                [{**base, **changes}],
                ensure_ascii=False,
            )

        cases = (
            (
                "unknown-enabled",
                task_json(enabeld=False),
                "第 1 个任务.*未知字段.*enabeld",
            ),
            (
                "null-id",
                task_json(id=None),
                "第 1 个任务.*id.*必须是字符串",
            ),
            (
                "numeric-id",
                task_json(id=7),
                "第 1 个任务.*id.*必须是字符串",
            ),
            (
                "null-function",
                task_json(function=None),
                "第 1 个任务.*function.*必须是字符串",
            ),
            (
                "array-function",
                task_json(function=["A()"]),
                "第 1 个任务.*function.*必须是字符串",
            ),
            (
                "null-document",
                task_json(document=None),
                "第 1 个任务.*document.*必须是字符串",
            ),
            (
                "numeric-source",
                task_json(source=7),
                "第 1 个任务.*source.*必须是字符串",
            ),
            (
                "object-prompt",
                task_json(prompt={"text": "do A"}),
                "第 1 个任务.*prompt.*必须是字符串",
            ),
            (
                "null-enabled",
                task_json(enabled=None),
                "第 1 个任务.*enabled.*JSON 布尔值",
            ),
            (
                "string-enabled",
                task_json(enabled="false"),
                "第 1 个任务.*enabled.*JSON 布尔值",
            ),
            (
                "document-alias-conflict",
                task_json(target="B.md"),
                "第 1 个任务.*不能同时包含 document 和 target",
            ),
            (
                "prompt-alias-conflict",
                task_json(prompt="first", custom_prompt="second"),
                "第 1 个任务.*不能同时包含 prompt 和 custom_prompt",
            ),
            (
                "wrapper-extra-field",
                json.dumps({"tasks": [base], "extra": True}),
                "顶层对象包含未知字段.*extra",
            ),
            (
                "duplicate-task-field",
                '[{"id":"task-a","function":"A()",'
                '"document":"A.md","enabled":false,"enabled":true}]',
                "重复字段.*enabled",
            ),
            (
                "nonstandard-number",
                '[{"id":"task-a","function":"A()",'
                '"document":"A.md","source":NaN}]',
                "非标准数值.*NaN",
            ),
        )
        for case, content, message in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                tasks_path = root / "tasks.json"
                tasks_path.write_text(content, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, message):
                    load_tasks(tasks_path, root)

    def test_load_json_accepts_strict_canonical_and_legacy_shapes(self) -> None:
        cases = (
            (
                "canonical-array",
                [
                    {
                        "id": "task-a",
                        "enabled": False,
                        "document": "A.md",
                        "function": "A()",
                    },
                    {
                        "id": "task-b",
                        "function": "B()",
                        "prompt": "prompt only",
                    },
                ],
                ((False, "A.md", ""), (True, "", "prompt only")),
            ),
            (
                "legacy-wrapper",
                {
                    "tasks": [
                        {
                            "custom_prompt": " legacy prompt ",
                            "target": " B.md ",
                            "function": " B() ",
                            "id": " task-b ",
                        }
                    ]
                },
                ((True, "B.md", "legacy prompt"),),
            ),
            ("empty-array", [], ()),
            ("empty-wrapper", {"tasks": []}, ()),
        )
        for case, payload, expected in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                tasks_path = root / "tasks.json"
                tasks_path.write_text(
                    json.dumps(payload, ensure_ascii=False),
                    encoding="utf-8",
                )
                tasks = load_tasks(tasks_path, root)
                self.assertEqual(
                    tuple(
                        (task.enabled, task.document, task.custom_prompt)
                        for task in tasks
                    ),
                    expected,
                )

    def test_cli_rejects_invalid_json_schema_before_creating_output(self) -> None:
        cases = (
            (
                "unknown-enabled",
                {
                    "id": "task-a",
                    "enabeld": False,
                    "document": "A.md",
                    "function": "A()",
                },
                "enabeld",
            ),
            (
                "null-id",
                {
                    "id": None,
                    "document": "A.md",
                    "function": "A()",
                },
                "id",
            ),
        )
        for case, task_payload, field in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                tasks_path = root / "tasks.json"
                tasks_path.write_text(
                    json.dumps([task_payload], ensure_ascii=False),
                    encoding="utf-8",
                )
                output_dir = root / "state"
                config_path = root / "config.json"
                config_path.write_text(
                    json.dumps(
                        {
                            "project_dir": str(project_dir),
                            "tasks_file": str(tasks_path),
                            "output_dir": str(output_dir),
                        }
                    ),
                    encoding="utf-8",
                )
                stdout = io.StringIO()
                stderr = io.StringIO()

                with mock.patch(
                    "run_batch.execute_batch"
                ) as execute_batch_mock, mock.patch(
                    "codex_batch.load_sdk_api"
                ) as load_sdk_api, contextlib.redirect_stdout(
                    stdout
                ), contextlib.redirect_stderr(
                    stderr
                ):
                    result = run_batch_main(["--config", str(config_path)])

                self.assertEqual(result, 2)
                self.assertEqual(stdout.getvalue(), "")
                self.assertIn(field, stderr.getvalue())
                self.assertFalse(output_dir.exists())
                execute_batch_mock.assert_not_called()
                load_sdk_api.assert_not_called()

    def test_two_tasks_are_two_turns_on_same_thread_and_resume_skips(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            tool_dir = root / "tool"
            tool_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=tool_dir / "config.json",
                project_dir=project_dir,
                tasks_file=tool_dir / "tasks.csv",
                output_dir=output_dir,
                thread_name="测试 Thread",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            world = FakeWorld()
            sdk = fake_sdk(world)

            result = execute_batch(settings, tasks, sdk_api=sdk)
            self.assertEqual(result, 0)
            self.assertEqual(world.codex_instances, 2)
            self.assertEqual(world.start_count, 1)
            self.assertEqual(world.resume_ids, ["thread-shared"])
            self.assertEqual(world.turn_ids, ["turn-1", "turn-2"])
            task_a_paths = self._attempt_artifact_relatives("task-a", 1)
            task_b_paths = self._attempt_artifact_relatives("task-b", 1)
            task_a_first_bytes = {
                relative: (output_dir / relative).read_bytes()
                for relative in task_a_paths
            }

            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.get_meta("thread_id"), "thread-shared")
                self.assertEqual(store.task("task-a")["status"], "SUCCEEDED")
                self.assertEqual(store.task("task-b")["status"], "SUCCEEDED")
                attempt_a = store.latest_attempt("task-a")
                self.assertEqual(
                    (
                        attempt_a["prompt_file"],
                        attempt_a["event_file"],
                        attempt_a["response_file"],
                    ),
                    task_a_paths,
                )
                self.assertNotEqual(
                    attempt_a["turn_id"],
                    store.latest_attempt("task-b")["turn_id"],
                )

            self.assertTrue((output_dir / task_a_paths[2]).is_file())
            self.assertTrue((output_dir / task_b_paths[2]).is_file())
            report = (output_dir / "chat_report.html").read_text(encoding="utf-8")
            self.assertIn("已完成 turn-1", report)
            self.assertIn("已完成 turn-2", report)
            private_files = [
                output_dir / ".codex-function-doc-batch-state.json",
                output_dir / "run.sqlite3",
                output_dir / "chat_report.html",
                output_dir / task_a_paths[0],
                output_dir / task_a_paths[1],
                output_dir / task_a_paths[2],
            ]
            for private_file in private_files:
                self.assertEqual(
                    stat.S_IMODE(private_file.stat().st_mode),
                    0o600,
                    str(private_file),
                )

            second_result = execute_batch(settings, tasks, sdk_api=sdk)
            self.assertEqual(second_result, 0)
            self.assertEqual(world.turn_ids, ["turn-1", "turn-2"])

            rerun_result = execute_batch(
                settings,
                tasks,
                limit=1,
                rerun=["task-a", "task-a"],
                sdk_api=sdk,
            )
            self.assertEqual(rerun_result, 0)
            self.assertEqual(world.turn_ids, ["turn-1", "turn-2", "turn-3"])
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "SUCCEEDED")
                self.assertEqual(store.latest_attempt("task-a")["attempt_no"], 2)
                first_attempt = store.conn.execute(
                    "SELECT * FROM attempts "
                    "WHERE task_id='task-a' AND attempt_no=1"
                ).fetchone()
                second_attempt = store.conn.execute(
                    "SELECT * FROM attempts "
                    "WHERE task_id='task-a' AND attempt_no=2"
                ).fetchone()
                first_paths = tuple(
                    first_attempt[field]
                    for field in ("prompt_file", "event_file", "response_file")
                )
                second_paths = tuple(
                    second_attempt[field]
                    for field in ("prompt_file", "event_file", "response_file")
                )
                self.assertEqual(first_paths, task_a_paths)
                self.assertTrue(set(first_paths).isdisjoint(second_paths))
            for relative, original in task_a_first_bytes.items():
                self.assertEqual((output_dir / relative).read_bytes(), original)
            for relative in second_paths:
                self.assertTrue((output_dir / relative).is_file())
            self.assertIn(
                "已完成 turn-3",
                (output_dir / second_paths[2]).read_text(encoding="utf-8"),
            )

    def test_reordered_slug_collision_preserves_historical_attempt_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="工件身份绑定测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task_a = TaskSpec(
                "collision/path",
                1,
                True,
                "A.md",
                "A()",
                "A.c",
            )
            task_b = TaskSpec(
                "collision path",
                2,
                True,
                "B.md",
                "B()",
                "B.c",
            )
            self.assertEqual(task_a.filename_slug, task_b.filename_slug)
            world = FakeWorld()
            sdk = fake_sdk(world)

            with contextlib.redirect_stdout(io.StringIO()):
                first_result = execute_batch(
                    settings,
                    [task_a, task_b],
                    limit=1,
                    sdk_api=sdk,
                )

            self.assertEqual(first_result, 0)
            with Store(output_dir / "run.sqlite3") as store:
                first_attempt = store.conn.execute(
                    "SELECT * FROM attempts "
                    "WHERE task_id=? AND attempt_no=1",
                    (task_a.task_id,),
                ).fetchone()
                self.assertIsNotNone(first_attempt)
                first_paths = tuple(
                    first_attempt[field]
                    for field in ("prompt_file", "event_file", "response_file")
                )
                self.assertEqual(first_attempt["final_response"], "已完成 turn-1")
            first_bytes = {
                relative: (output_dir / relative).read_bytes()
                for relative in first_paths
            }

            reordered_a = TaskSpec(
                task_a.task_id,
                2,
                task_a.enabled,
                task_a.document,
                task_a.function,
                task_a.source,
                task_a.custom_prompt,
            )
            reordered_b = TaskSpec(
                task_b.task_id,
                1,
                task_b.enabled,
                task_b.document,
                task_b.function,
                task_b.source,
                task_b.custom_prompt,
            )
            with contextlib.redirect_stdout(io.StringIO()):
                second_result = execute_batch(
                    settings,
                    [reordered_b, reordered_a],
                    limit=1,
                    sdk_api=sdk,
                )

            self.assertEqual(second_result, 0)
            self.assertEqual(world.turn_ids, ["turn-1", "turn-2"])
            with Store(output_dir / "run.sqlite3") as store:
                first_attempt = store.conn.execute(
                    "SELECT * FROM attempts "
                    "WHERE task_id=? AND attempt_no=1",
                    (task_a.task_id,),
                ).fetchone()
                second_attempt = store.conn.execute(
                    "SELECT * FROM attempts "
                    "WHERE task_id=? AND attempt_no=1",
                    (task_b.task_id,),
                ).fetchone()
                self.assertEqual(
                    tuple(
                        first_attempt[field]
                        for field in (
                            "prompt_file",
                            "event_file",
                            "response_file",
                        )
                    ),
                    first_paths,
                )
                second_paths = tuple(
                    second_attempt[field]
                    for field in ("prompt_file", "event_file", "response_file")
                )
                self.assertTrue(set(first_paths).isdisjoint(second_paths))
                self.assertEqual(first_attempt["final_response"], "已完成 turn-1")
                self.assertEqual(second_attempt["final_response"], "已完成 turn-2")

            for relative, original in first_bytes.items():
                self.assertEqual((output_dir / relative).read_bytes(), original)
            for relative in second_paths:
                self.assertTrue((output_dir / relative).is_file())
            self.assertIn(
                "[batch-task-id: collision path]",
                (output_dir / second_paths[0]).read_text(encoding="utf-8"),
            )
            self.assertIn(
                "turn-2",
                (output_dir / second_paths[1]).read_text(encoding="utf-8"),
            )
            self.assertIn(
                "已完成 turn-2",
                (output_dir / second_paths[2]).read_text(encoding="utf-8"),
            )

    def test_artifact_basename_binds_full_task_identity(self) -> None:
        first = attempt_artifact_basename("collision/path", 1)
        second = attempt_artifact_basename("collision path", 1)

        self.assertNotEqual(first, second)
        self.assertEqual(
            first,
            attempt_artifact_basename("collision/path", 1),
        )
        self.assertNotEqual(
            first,
            attempt_artifact_basename("collision/path", 2),
        )
        self.assertRegex(first, r"^v2-task-[0-9a-f]{64}-a1$")
        for invalid_attempt_no in (True, 1.0, 0, -1, 1 << 63):
            with self.subTest(attempt_no=invalid_attempt_no):
                with self.assertRaisesRegex(ValueError, "SQLite 正整数"):
                    attempt_artifact_basename(
                        "collision/path",
                        invalid_attempt_no,
                    )

    def test_start_attempt_rejects_reused_historical_artifact_path(self) -> None:
        owner_paths = {
            "prompt_file": "prompts/owner.txt",
            "event_file": "events/owner.jsonl",
            "response_file": "responses/owner.md",
        }
        cases = (
            ("prompt", "prompt_file", owner_paths["prompt_file"]),
            ("event", "event_file", owner_paths["event_file"]),
            ("response", "response_file", owner_paths["response_file"]),
            ("cross-field", "prompt_file", owner_paths["response_file"]),
        )
        for case, field, occupied_path in cases:
            with self.subTest(
                case=case
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                task_a = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                task_b = TaskSpec("task-b", 2, True, "B.md", "B()", "B.c")
                candidate_paths = {
                    "prompt_file": "prompts/task-b.txt",
                    "event_file": "events/task-b.jsonl",
                    "response_file": "responses/task-b.md",
                }
                candidate_paths[field] = occupied_path
                with Store(root / "run.sqlite3") as store:
                    store.sync_tasks([task_a, task_b])
                    store.start_attempt(
                        task_a,
                        attempt_no=1,
                        prompt=build_prompt(task_a, 1),
                        agents_snapshot=[],
                        **owner_paths,
                    )

                    with self.assertRaisesRegex(
                        TurnCheckpointConflict,
                        "拒绝复用历史工件",
                    ):
                        store.start_attempt(
                            task_b,
                            attempt_no=1,
                            prompt=build_prompt(task_b, 1),
                            agents_snapshot=[],
                            **candidate_paths,
                        )

                    self.assertEqual(
                        store.task(task_b.task_id)["status"],
                        "PENDING",
                    )
                    self.assertEqual(
                        store.task(task_b.task_id)["latest_attempt"],
                        0,
                    )
                    self.assertEqual(
                        store.conn.execute(
                            "SELECT COUNT(*) FROM attempts WHERE task_id=?",
                            (task_b.task_id,),
                        ).fetchone()[0],
                        0,
                    )
                    self.assertFalse(store.conn.in_transaction)

    def test_start_attempt_rejects_non_sqlite_attempt_number(self) -> None:
        for invalid_attempt_no in (True, 1.0, 0, -1, 1 << 63):
            with self.subTest(
                attempt_no=invalid_attempt_no
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                with Store(root / "run.sqlite3") as store:
                    store.sync_tasks([task])

                    with self.assertRaisesRegex(ValueError, "SQLite 正整数"):
                        store.start_attempt(
                            task,
                            attempt_no=invalid_attempt_no,
                            prompt=build_prompt(task, 1),
                            agents_snapshot=[],
                            prompt_file="prompts/task-a.txt",
                            event_file="events/task-a.jsonl",
                            response_file="responses/task-a.md",
                        )

                    self.assertEqual(store.task(task.task_id)["status"], "PENDING")
                    self.assertEqual(store.task(task.task_id)["latest_attempt"], 0)
                    self.assertEqual(
                        store.conn.execute(
                            "SELECT COUNT(*) FROM attempts WHERE task_id=?",
                            (task.task_id,),
                        ).fetchone()[0],
                        0,
                    )
                    self.assertFalse(store.conn.in_transaction)

    def test_run_one_task_reserves_paths_before_publishing_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="Prompt 预留顺序测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            owner = TaskSpec("owner", 1, True, "A.md", "A()", "A.c")
            target = TaskSpec("target", 2, True, "B.md", "B()", "B.c")
            target_paths = self._attempt_artifact_relatives(target.task_id, 1)
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([owner, target])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    owner,
                    attempt_no=1,
                    prompt=build_prompt(owner, 1),
                    agents_snapshot=[],
                    prompt_file=target_paths[0],
                    event_file=target_paths[1],
                    response_file=target_paths[2],
                )
                atomic_write_text(
                    output_dir,
                    target_paths[0],
                    "HISTORICAL PROMPT\n",
                    field="prompt_file",
                    expected_directory="prompts",
                )
                prompt_path = output_dir / target_paths[0]
                original_inode = prompt_path.stat().st_ino
                world = FakeWorld()

                with self.assertRaisesRegex(
                    TurnCheckpointConflict,
                    "拒绝复用历史工件",
                ):
                    run_one_task(
                        store,
                        settings,
                        target,
                        sdk_api=fake_sdk(world),
                        stream_events=True,
                    )

                self.assertEqual(world.codex_instances, 0)
                self.assertEqual(world.turn_request_count, 0)
                self.assertEqual(
                    prompt_path.read_text(encoding="utf-8"),
                    "HISTORICAL PROMPT\n",
                )
                self.assertEqual(prompt_path.stat().st_ino, original_inode)
                self.assertEqual(store.task(target.task_id)["status"], "PENDING")
                self.assertEqual(store.task(target.task_id)["latest_attempt"], 0)
                self.assertEqual(
                    store.conn.execute(
                        "SELECT COUNT(*) FROM attempts WHERE task_id=?",
                        (target.task_id,),
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    list(prompt_path.parent.glob(".codex-batch-*.tmp")),
                    [],
                )

    def test_new_thread_keeps_old_id_when_sdk_load_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="SDK 加载失败保留 Thread 测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.set_meta("thread_id", "thread-old")

            with mock.patch(
                "codex_batch.load_sdk_api",
                side_effect=RuntimeError("injected SDK load failure"),
            ):
                with self.assertRaisesRegex(RuntimeError, "SDK load failure"):
                    execute_batch(settings, [task], new_thread=True)

            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.get_meta("thread_id"), "thread-old")
                self.assertEqual(store.task(task.task_id)["status"], "PENDING")
                self.assertIsNone(store.latest_attempt(task.task_id))

    def test_new_thread_keeps_old_id_when_nothing_is_selected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="无待处理任务保留 Thread 测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.set_meta("thread_id", "thread-old")
                with store.conn:
                    store.conn.execute(
                        "UPDATE tasks SET status='SUCCEEDED' WHERE task_id=?",
                        (task.task_id,),
                    )

            world = FakeWorld()
            with contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(
                    settings,
                    [task],
                    new_thread=True,
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 0)
            self.assertEqual(world.codex_instances, 0)
            self.assertEqual(world.start_count, 0)
            self.assertEqual(world.turn_request_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.get_meta("thread_id"), "thread-old")

    def test_new_thread_keeps_old_id_when_thread_start_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="Thread 创建失败保留旧 ID 测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.set_meta("thread_id", "thread-old")

            world = FakeWorld()
            world.thread_start_error_starts.add(1)
            with contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(
                    settings,
                    [task],
                    new_thread=True,
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 1)
            self.assertEqual(world.start_count, 1)
            self.assertEqual(world.resume_ids, [])
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.get_meta("thread_id"), "thread-old")
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "FAILED_BEFORE_REQUEST")
                self.assertIsNone(attempt["thread_id"])

    def test_new_thread_adopts_new_id_before_turn_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="新 Thread 提交边界测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.set_meta("thread_id", "thread-old")

            world = FakeWorld()
            world.thread_start_ids[1] = "thread-new"
            with mock.patch(
                "codex_batch.turn_kwargs",
                side_effect=RuntimeError("injected turn option failure"),
            ):
                with contextlib.redirect_stdout(io.StringIO()):
                    result = execute_batch(
                        settings,
                        [task],
                        new_thread=True,
                        sdk_api=fake_sdk(world),
                    )

            self.assertEqual(result, 1)
            self.assertEqual(world.start_count, 1)
            self.assertEqual(world.resume_ids, [])
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.get_meta("thread_id"), "thread-new")
                self.assertEqual(
                    store.latest_attempt(task.task_id)["status"],
                    "FAILED_BEFORE_REQUEST",
                )

    def test_new_thread_intent_survives_pre_start_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="新 Thread 意图延续测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
                TaskSpec("task-c", 3, True, "C.md", "C()", "C.c"),
            ]
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks(tasks)
                store.set_meta("project_dir", str(project_dir))
                store.set_meta("thread_id", "thread-old")

            world = FakeWorld()
            world.thread_start_error_starts.add(1)
            world.thread_start_ids[2] = "thread-new"
            with contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(
                    settings,
                    tasks,
                    new_thread=True,
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 1)
            self.assertEqual(world.start_count, 2)
            self.assertEqual(world.resume_ids, ["thread-new"])
            self.assertEqual(world.turn_request_count, 2)
            self.assertEqual(world.remote_accept_count, 2)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.get_meta("thread_id"), "thread-new")
                self.assertEqual(store.task("task-a")["status"], "FAILED_BEFORE_REQUEST")
                self.assertEqual(store.task("task-b")["status"], "SUCCEEDED")
                self.assertEqual(store.task("task-c")["status"], "SUCCEEDED")

    def test_new_thread_stops_batch_on_adoption_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="新 Thread 并发冲突停止测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks(tasks)
                store.set_meta("project_dir", str(project_dir))
                store.set_meta("thread_id", "thread-old")

            original_adopt_thread_id = Store.adopt_thread_id
            original_finish_attempt = Store.finish_attempt
            finish_calls: list[str] = []

            def inject_conflict(
                store: Store,
                *,
                expected_old_thread_id: str | None,
                new_thread_id: str,
            ) -> None:
                store.set_meta("thread_id", "thread-external")
                original_adopt_thread_id(
                    store,
                    expected_old_thread_id=expected_old_thread_id,
                    new_thread_id=new_thread_id,
                )

            def track_finish_attempt(store: Store, *args, **kwargs) -> None:
                finish_calls.append(str(args[0]))
                original_finish_attempt(store, *args, **kwargs)

            world = FakeWorld()
            world.thread_start_ids[1] = "thread-new"
            with mock.patch.object(
                Store,
                "adopt_thread_id",
                new=inject_conflict,
            ), mock.patch.object(
                Store,
                "finish_attempt",
                new=track_finish_attempt,
            ):
                with contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(
                        ThreadAdoptionConflict,
                        "已发生变化",
                    ):
                        execute_batch(
                            settings,
                            tasks,
                            new_thread=True,
                            sdk_api=fake_sdk(world),
                        )

            self.assertEqual(world.start_count, 1)
            self.assertEqual(world.resume_ids, [])
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            self.assertEqual(finish_calls, [])
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.get_meta("thread_id"), "thread-external")
                self.assertEqual(
                    store.task("task-a")["status"],
                    "TURN_NOT_REQUESTED",
                )
                attempt = store.latest_attempt("task-a")
                self.assertEqual(attempt["status"], "TURN_NOT_REQUESTED")
                self.assertIsNone(attempt["thread_id"])
                self.assertIsNone(attempt["turn_id"])
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

            retry_world = FakeWorld()
            with contextlib.redirect_stdout(
                io.StringIO()
            ), contextlib.redirect_stderr(io.StringIO()):
                result = execute_batch(
                    settings,
                    tasks,
                    sdk_api=fake_sdk(retry_world),
                )
            self.assertEqual(result, 2)
            self.assertEqual(retry_world.codex_instances, 0)
            self.assertEqual(retry_world.turn_request_count, 0)

            explicit_retry_world = FakeWorld()
            with contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(
                    settings,
                    tasks,
                    limit=1,
                    retry_incomplete=["task-a"],
                    sdk_api=fake_sdk(explicit_retry_world),
                )
            self.assertEqual(result, 0)
            self.assertEqual(
                explicit_retry_world.resume_ids,
                ["thread-external"],
            )
            self.assertEqual(explicit_retry_world.turn_request_count, 1)
            self.assertEqual(explicit_retry_world.remote_accept_count, 1)
            with Store(output_dir / "run.sqlite3") as store:
                attempts = list(
                    store.conn.execute(
                        """
                        SELECT attempt_no, status
                        FROM attempts WHERE task_id=?
                        ORDER BY attempt_no
                        """,
                        ("task-a",),
                    )
                )
                self.assertEqual(
                    [(row["attempt_no"], row["status"]) for row in attempts],
                    [
                        (1, "ABANDONED_BEFORE_REQUEST"),
                        (2, "SUCCEEDED"),
                    ],
                )
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

    def test_retry_incomplete_allows_only_new_pre_request_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="请求前安全重试测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-task-a-a1.txt",
                    event_file="events/001-task-a-a1.jsonl",
                    response_file="responses/001-task-a-a1.md",
                )
                self.assertEqual(
                    store.latest_attempt(task.task_id)["status"],
                    "TURN_NOT_REQUESTED",
                )

            world = FakeWorld()
            with contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(
                    settings,
                    [task],
                    retry_incomplete=[task.task_id, task.task_id],
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 0)
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            with Store(output_dir / "run.sqlite3") as store:
                attempts = list(
                    store.conn.execute(
                        "SELECT attempt_no, status, thread_id, turn_id "
                        "FROM attempts WHERE task_id=? ORDER BY attempt_no",
                        (task.task_id,),
                    )
                )
                self.assertEqual(
                    [tuple(row) for row in attempts],
                    [
                        (1, "ABANDONED_BEFORE_REQUEST", None, None),
                        (2, "SUCCEEDED", "thread-shared", "turn-1"),
                    ],
                )

    def test_retry_option_batch_rolls_back_when_any_argument_is_invalid(
        self,
    ) -> None:
        cases = [
            (
                "rerun-later-invalid",
                ["task-a", "task-b"],
                [],
                "SUCCEEDED",
            ),
            (
                "retry-later-invalid",
                [],
                ["task-a", "task-b"],
                "TURN_NOT_REQUESTED",
            ),
            (
                "cross-list-distinct",
                ["task-b"],
                ["task-a"],
                "TURN_NOT_REQUESTED",
            ),
            (
                "cross-list-overlap",
                ["task-a"],
                ["task-a"],
                "TURN_NOT_REQUESTED",
            ),
        ]

        for name, rerun, retry_incomplete, expected_status in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="批量状态原子回滚测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=False,
                )
                task_a = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                task_b = TaskSpec("task-b", 2, True, "B.md", "B()", "B.c")
                tasks = [task_a, task_b]
                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    store.sync_tasks(tasks)
                    store.set_meta("project_dir", str(project_dir))
                    if expected_status == "SUCCEEDED":
                        with store.conn:
                            store.conn.execute(
                                "UPDATE tasks SET status='SUCCEEDED' WHERE task_id=?",
                                (task_a.task_id,),
                            )
                    else:
                        store.start_attempt(
                            task_a,
                            attempt_no=1,
                            prompt=build_prompt(task_a, 1),
                            agents_snapshot=[],
                            prompt_file="prompts/001-task-a-a1.txt",
                            event_file="events/001-task-a-a1.jsonl",
                            response_file="responses/001-task-a-a1.md",
                        )

                world = FakeWorld()
                with self.assertRaises(ValueError):
                    execute_batch(
                        settings,
                        tasks,
                        rerun=rerun,
                        retry_incomplete=retry_incomplete,
                        sdk_api=fake_sdk(world),
                    )

                self.assertEqual(world.codex_instances, 0)
                self.assertEqual(world.start_count, 0)
                self.assertEqual(world.turn_request_count, 0)
                self.assertEqual(world.remote_accept_count, 0)
                self.assertEqual(world.turn_ids, [])
                with Store(output_dir / "run.sqlite3") as store:
                    self.assertEqual(store.task(task_a.task_id)["status"], expected_status)
                    self.assertEqual(
                        store.task(task_b.task_id)["status"],
                        "PENDING",
                    )
                    attempt = store.latest_attempt(task_a.task_id)
                    if expected_status == "TURN_NOT_REQUESTED":
                        self.assertIsNotNone(attempt)
                        self.assertEqual(attempt["status"], "TURN_NOT_REQUESTED")
                        self.assertIsNone(attempt["completed_at"])
                    else:
                        self.assertIsNone(attempt)

    def test_retry_option_batch_rolls_back_when_other_unresolved_remains(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="未对账任务原子回滚测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task_a = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            task_b = TaskSpec("task-b", 2, True, "B.md", "B()", "B.c")
            tasks = [task_a, task_b]
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks(tasks)
                store.set_meta("project_dir", str(project_dir))
                for task in tasks:
                    store.start_attempt(
                        task,
                        attempt_no=1,
                        prompt=build_prompt(task, 1),
                        agents_snapshot=[],
                        prompt_file=f"prompts/{task.task_id}.txt",
                        event_file=f"events/{task.task_id}.jsonl",
                        response_file=f"responses/{task.task_id}.md",
                    )
                store.mark_turn_start_requested(
                    task_b.task_id,
                    1,
                    thread_id="thread-old",
                )
                store.mark_turn_started(
                    task_b.task_id,
                    1,
                    thread_id="thread-old",
                    turn_id="turn-old",
                )

            world = FakeWorld()
            error_output = io.StringIO()
            with contextlib.redirect_stderr(error_output):
                result = execute_batch(
                    settings,
                    tasks,
                    retry_incomplete=[task_a.task_id],
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 2)
            self.assertIn("task-b/a1:RUNNING", error_output.getvalue())
            self.assertEqual(world.codex_instances, 0)
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(
                    store.task(task_a.task_id)["status"],
                    "TURN_NOT_REQUESTED",
                )
                attempt_a = store.latest_attempt(task_a.task_id)
                self.assertEqual(attempt_a["status"], "TURN_NOT_REQUESTED")
                self.assertIsNone(attempt_a["completed_at"])
                self.assertEqual(store.task(task_b.task_id)["status"], "RUNNING")
                self.assertEqual(store.latest_attempt(task_b.task_id)["status"], "RUNNING")

    def test_retry_option_batch_rolls_back_after_mid_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="批量状态写入故障回滚测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks(tasks)
                store.set_meta("project_dir", str(project_dir))
                with store.conn:
                    store.conn.execute(
                        "UPDATE tasks SET status='SUCCEEDED' "
                        "WHERE task_id IN ('task-a', 'task-b')"
                    )

            original_set_pending = Store._set_pending_locked
            write_count = 0

            def fail_after_second_write(
                store: Store,
                task_id: str,
                *,
                now: str,
            ) -> None:
                nonlocal write_count
                original_set_pending(store, task_id, now=now)
                write_count += 1
                if write_count == 2:
                    raise RuntimeError("injected state write failure")

            world = FakeWorld()
            with mock.patch.object(
                Store,
                "_set_pending_locked",
                new=fail_after_second_write,
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "injected state write failure",
                ):
                    execute_batch(
                        settings,
                        tasks,
                        rerun=["task-a", "task-b"],
                        sdk_api=fake_sdk(world),
                    )

            self.assertEqual(write_count, 2)
            self.assertEqual(world.codex_instances, 0)
            self.assertEqual(world.start_count, 0)
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertFalse(store.conn.in_transaction)
                self.assertEqual(store.task("task-a")["status"], "SUCCEEDED")
                self.assertEqual(store.task("task-b")["status"], "SUCCEEDED")

    def test_retry_incomplete_refuses_legacy_starting_without_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="旧版 STARTING 拒绝测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-task-a-a1.txt",
                    event_file="events/001-task-a-a1.jsonl",
                    response_file="responses/001-task-a-a1.md",
                )
                with store.conn:
                    store.conn.execute(
                        "UPDATE attempts SET status='STARTING' WHERE task_id=?",
                        (task.task_id,),
                    )
                    store.conn.execute(
                        "UPDATE tasks SET status='STARTING' WHERE task_id=?",
                        (task.task_id,),
                    )

            world = FakeWorld()
            with self.assertRaisesRegex(ValueError, "不能使用 --retry-incomplete"):
                execute_batch(
                    settings,
                    [task],
                    retry_incomplete=[task.task_id],
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(world.turn_request_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task(task.task_id)["status"], "STARTING")
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "STARTING")
                self.assertIsNone(attempt["thread_id"])
                self.assertIsNone(attempt["turn_id"])

    def test_legacy_abandoned_attempt_still_blocks_new_turn(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="历史 ABANDONED 拒绝测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            disabled_task = TaskSpec(
                "task-a",
                1,
                False,
                "A.md",
                "A()",
                "A.c",
            )
            next_task = TaskSpec("task-b", 2, True, "B.md", "B()", "B.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-task-a-a1.txt",
                    event_file="events/001-task-a-a1.jsonl",
                    response_file="responses/001-task-a-a1.md",
                )
                with store.conn:
                    store.conn.execute(
                        "UPDATE attempts SET status='ABANDONED' WHERE task_id=?",
                        (task.task_id,),
                    )
                    store.conn.execute(
                        "UPDATE tasks SET enabled=0, status='DISABLED' WHERE task_id=?",
                        (task.task_id,),
                    )

            world = FakeWorld()
            error_output = io.StringIO()
            with contextlib.redirect_stderr(error_output):
                result = execute_batch(
                    settings,
                    [disabled_task, next_task],
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 2)
            self.assertEqual(world.turn_request_count, 0)
            self.assertIn("task-a/a1:ABANDONED", error_output.getvalue())
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task(task.task_id)["status"], "DISABLED")
                self.assertEqual(
                    store.latest_attempt(task.task_id)["status"],
                    "ABANDONED",
                )
                self.assertEqual(store.task(next_task.task_id)["status"], "PENDING")
                self.assertIsNone(store.latest_attempt(next_task.task_id))

    def test_legacy_failed_blocks_ordinary_rerun_and_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="历史 FAILED 拒绝测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            next_task = TaskSpec("task-b", 2, True, "B.md", "B()", "B.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-task-a-a1.txt",
                    event_file="events/001-task-a-a1.jsonl",
                    response_file="responses/001-task-a-a1.md",
                )
                with store.conn:
                    store.conn.execute(
                        "UPDATE attempts SET status='FAILED', "
                        "error_json='\"legacy local-looking error\"' "
                        "WHERE task_id=?",
                        (task.task_id,),
                    )
                    store.conn.execute(
                        "UPDATE tasks SET status='FAILED' WHERE task_id=?",
                        (task.task_id,),
                    )

            world = FakeWorld()
            error_output = io.StringIO()
            with contextlib.redirect_stderr(error_output):
                result = execute_batch(
                    settings,
                    [task, next_task],
                    sdk_api=fake_sdk(world),
                )
            self.assertEqual(result, 2)
            self.assertIn("task-a/a1:FAILED", error_output.getvalue())

            with contextlib.redirect_stderr(io.StringIO()):
                rerun_result = execute_batch(
                    settings,
                    [task, next_task],
                    rerun=[task.task_id],
                    sdk_api=fake_sdk(world),
                )
            self.assertEqual(rerun_result, 2)

            with self.assertRaisesRegex(ValueError, "不能使用 --retry-incomplete"):
                execute_batch(
                    settings,
                    [task, next_task],
                    retry_incomplete=[task.task_id],
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(world.codex_instances, 0)
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task(task.task_id)["status"], "FAILED")
                self.assertEqual(
                    store.latest_attempt(task.task_id)["status"],
                    "FAILED",
                )
                self.assertEqual(store.task(next_task.task_id)["status"], "PENDING")
                self.assertIsNone(store.latest_attempt(next_task.task_id))

    def test_legacy_failed_attempt_behind_success_still_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="隐藏历史 FAILED 拒绝测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            next_task = TaskSpec("task-b", 2, True, "B.md", "B()", "B.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-task-a-a1.txt",
                    event_file="events/001-task-a-a1.jsonl",
                    response_file="responses/001-task-a-a1.md",
                )
                with store.conn:
                    store.conn.execute(
                        "UPDATE attempts SET status='FAILED' WHERE task_id=?",
                        (task.task_id,),
                    )
                # 直接注入非法历史快照：公开 start_attempt() 不应允许从
                # TURN_NOT_REQUESTED 再启动一个 Attempt。
                with store.conn:
                    store.conn.execute(
                        """
                        INSERT INTO attempts(
                            task_id, attempt_no, status, started_at, prompt,
                            agents_snapshot_json, prompt_file, event_file,
                            response_file
                        ) VALUES(?, 2, 'TURN_NOT_REQUESTED', ?, ?, '[]', ?, ?, ?)
                        """,
                        (
                            task.task_id,
                            "injected-started-at",
                            build_prompt(task, 2),
                            "prompts/001-task-a-a2.txt",
                            "events/001-task-a-a2.jsonl",
                            "responses/001-task-a-a2.md",
                        ),
                    )
                    store.conn.execute(
                        """
                        UPDATE tasks
                        SET status='TURN_NOT_REQUESTED', latest_attempt=2
                        WHERE task_id=?
                        """,
                        (task.task_id,),
                    )
                store.mark_turn_start_requested(
                    task.task_id,
                    2,
                    thread_id="thread-new",
                )
                store.mark_turn_started(
                    task.task_id,
                    2,
                    thread_id="thread-new",
                    turn_id="turn-new",
                )
                store.finish_attempt(
                    task.task_id,
                    2,
                    expected_task_status="RUNNING",
                    expected_attempt_status="RUNNING",
                    expected_turn_id="turn-new",
                    task_status="SUCCEEDED",
                    attempt_status="SUCCEEDED",
                    final_response="later attempt succeeded",
                    error=None,
                    usage=None,
                    duration_ms=1,
                )

            world = FakeWorld()
            error_output = io.StringIO()
            with contextlib.redirect_stderr(error_output):
                result = execute_batch(
                    settings,
                    [task, next_task],
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 2)
            self.assertEqual(world.turn_request_count, 0)
            self.assertIn("task-a/a1:FAILED", error_output.getvalue())
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task(task.task_id)["status"], "SUCCEEDED")
                self.assertEqual(store.task(next_task.task_id)["status"], "PENDING")
                attempts = list(
                    store.conn.execute(
                        "SELECT attempt_no, status FROM attempts "
                        "WHERE task_id=? ORDER BY attempt_no",
                        (task.task_id,),
                    )
                )
                self.assertEqual(
                    [tuple(row) for row in attempts],
                    [(1, "FAILED"), (2, "SUCCEEDED")],
                )

    def test_unknown_task_or_attempt_status_blocks_before_codex(self) -> None:
        for location, unknown_status, rerun in (
            ("task", "FUTURE_TASK_STATE", False),
            ("disabled_task", "FUTURE_DISABLED_TASK_STATE", False),
            ("latest_attempt", "FUTURE_LATEST_STATE", True),
            ("hidden_attempt", "FUTURE_HIDDEN_STATE", True),
        ):
            with self.subTest(
                location=location
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                (project_dir / "AGENTS.md").write_text(
                    "# Rules\n",
                    encoding="utf-8",
                )
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="未知本地状态阻断测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=False,
                )
                task_a = TaskSpec(
                    "task-a",
                    1,
                    location != "disabled_task",
                    "A.md",
                    "A()",
                    "A.c",
                )
                task_b = TaskSpec("task-b", 2, True, "B.md", "B()", "B.c")
                tasks = [task_a, task_b]
                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    store.sync_tasks(tasks)
                    store.set_meta("project_dir", str(project_dir))
                    if location in {"task", "disabled_task"}:
                        with store.conn:
                            store.conn.execute(
                                "UPDATE tasks SET status=? WHERE task_id=?",
                                (unknown_status, task_a.task_id),
                            )
                    else:
                        store.start_attempt(
                            task_a,
                            attempt_no=1,
                            prompt=build_prompt(task_a, 1),
                            agents_snapshot=[],
                            prompt_file="prompts/task-a-a1.txt",
                            event_file="events/task-a-a1.jsonl",
                            response_file="responses/task-a-a1.md",
                        )
                        with store.conn:
                            store.conn.execute(
                                "UPDATE attempts SET status=? "
                                "WHERE task_id=? AND attempt_no=1",
                                (unknown_status, task_a.task_id),
                            )
                            if location == "hidden_attempt":
                                store.conn.execute(
                                    """
                                    INSERT INTO attempts(
                                        task_id, attempt_no, status, started_at,
                                        prompt, agents_snapshot_json, prompt_file,
                                        event_file, response_file
                                    ) VALUES(
                                        ?, 2, 'SUCCEEDED', ?, ?, '[]', ?, ?, ?
                                    )
                                    """,
                                    (
                                        task_a.task_id,
                                        "injected-started-at",
                                        build_prompt(task_a, 2),
                                        "prompts/task-a-a2.txt",
                                        "events/task-a-a2.jsonl",
                                        "responses/task-a-a2.md",
                                    ),
                                )
                                store.conn.execute(
                                    "UPDATE tasks SET status='SUCCEEDED', "
                                    "latest_attempt=2 WHERE task_id=?",
                                    (task_a.task_id,),
                                )
                            else:
                                store.conn.execute(
                                    "UPDATE tasks SET status='SUCCEEDED' "
                                    "WHERE task_id=?",
                                    (task_a.task_id,),
                                )

                world = FakeWorld()
                error_output = io.StringIO()
                with contextlib.redirect_stderr(error_output):
                    result = execute_batch(
                        settings,
                        tasks,
                        rerun=[task_a.task_id] if rerun else [],
                        sdk_api=fake_sdk(world),
                    )

                self.assertEqual(result, 2)
                self.assertIn(unknown_status, error_output.getvalue())
                self.assertIn("禁止使用 --rerun", error_output.getvalue())
                self.assertEqual(world.codex_instances, 0)
                self.assertEqual(world.start_count, 0)
                self.assertEqual(world.turn_request_count, 0)
                self.assertEqual(world.remote_accept_count, 0)
                self.assertEqual(world.turn_ids, [])

                with Store(output_dir / "run.sqlite3") as store:
                    expected_task_status = (
                        unknown_status
                        if location in {"task", "disabled_task"}
                        else "SUCCEEDED"
                    )
                    self.assertEqual(
                        store.task(task_a.task_id)["status"],
                        expected_task_status,
                    )
                    attempts = list(
                        store.conn.execute(
                            "SELECT attempt_no, status FROM attempts "
                            "WHERE task_id=? ORDER BY attempt_no",
                            (task_a.task_id,),
                        )
                    )
                    expected_attempts = []
                    if location == "latest_attempt":
                        expected_attempts = [(1, unknown_status)]
                    elif location == "hidden_attempt":
                        expected_attempts = [
                            (1, unknown_status),
                            (2, "SUCCEEDED"),
                        ]
                    self.assertEqual(
                        [tuple(row) for row in attempts],
                        expected_attempts,
                    )
                    self.assertEqual(store.task(task_b.task_id)["status"], "PENDING")
                    self.assertIsNone(store.latest_attempt(task_b.task_id))

    def test_sync_tasks_refuses_to_overwrite_unknown_task_status(self) -> None:
        for change in ("fingerprint", "enabled"):
            with self.subTest(
                change=change
            ), tempfile.TemporaryDirectory() as directory:
                db_path = Path(directory) / "run.sqlite3"
                original = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                changed = (
                    TaskSpec("task-a", 1, True, "B.md", "A()", "A.c")
                    if change == "fingerprint"
                    else TaskSpec("task-a", 1, False, "A.md", "A()", "A.c")
                )
                with Store(db_path) as store:
                    store.sync_tasks([original])
                    with store.conn:
                        store.conn.execute(
                            "UPDATE tasks SET status='FUTURE_TASK_STATE' "
                            "WHERE task_id=?",
                            (original.task_id,),
                        )
                    task_before = dict(store.task(original.task_id))

                    with self.assertRaisesRegex(ValueError, "未完成任务"):
                        store.sync_tasks([changed])

                    self.assertFalse(store.conn.in_transaction)
                    self.assertEqual(
                        dict(store.task(original.task_id)),
                        task_before,
                    )

    def test_start_attempt_rejects_unknown_existing_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            with Store(db_path) as store:
                store.sync_tasks([task])
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/task-a-a1.txt",
                    event_file="events/task-a-a1.jsonl",
                    response_file="responses/task-a-a1.md",
                )
                with store.conn:
                    store.conn.execute(
                        "UPDATE attempts SET status='FUTURE_ATTEMPT_STATE' "
                        "WHERE task_id=? AND attempt_no=1",
                        (task.task_id,),
                    )
                    store.conn.execute(
                        "UPDATE tasks SET status='PENDING' WHERE task_id=?",
                        (task.task_id,),
                    )
                task_before = dict(store.task(task.task_id))
                attempts_before = [
                    tuple(row)
                    for row in store.conn.execute(
                        "SELECT attempt_no, status FROM attempts "
                        "WHERE task_id=? ORDER BY attempt_no",
                        (task.task_id,),
                    )
                ]

                with self.assertRaises(TurnCheckpointConflict):
                    store.start_attempt(
                        task,
                        attempt_no=2,
                        prompt=build_prompt(task, 2),
                        agents_snapshot=[],
                        prompt_file="prompts/task-a-a2.txt",
                        event_file="events/task-a-a2.jsonl",
                        response_file="responses/task-a-a2.md",
                    )

                self.assertFalse(store.conn.in_transaction)
                self.assertEqual(dict(store.task(task.task_id)), task_before)
                attempts_after = [
                    tuple(row)
                    for row in store.conn.execute(
                        "SELECT attempt_no, status FROM attempts "
                        "WHERE task_id=? ORDER BY attempt_no",
                        (task.task_id,),
                    )
                ]
                self.assertEqual(attempts_after, attempts_before)

    def test_pre_request_failure_uses_provenance_status_and_retries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="请求前失败状态测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            world = FakeWorld()
            world.thread_start_error_starts.add(1)

            with contextlib.redirect_stdout(io.StringIO()):
                first_result = execute_batch(
                    settings,
                    [task],
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(first_result, 1)
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(
                    store.task(task.task_id)["status"],
                    "FAILED_BEFORE_REQUEST",
                )
                self.assertEqual(attempt["status"], "FAILED_BEFORE_REQUEST")
                self.assertIsNone(attempt["thread_id"])
                self.assertIsNone(attempt["turn_id"])

            with contextlib.redirect_stdout(io.StringIO()):
                second_result = execute_batch(
                    settings,
                    [task],
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(second_result, 0)
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            with Store(output_dir / "run.sqlite3") as store:
                attempts = list(
                    store.conn.execute(
                        "SELECT attempt_no, status FROM attempts "
                        "WHERE task_id=? ORDER BY attempt_no",
                        (task.task_id,),
                    )
                )
                self.assertEqual(
                    [tuple(row) for row in attempts],
                    [(1, "FAILED_BEFORE_REQUEST"), (2, "SUCCEEDED")],
                )

    def test_rerun_cannot_reset_running_task(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="rerun 保护测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-task-a-a1.txt",
                    event_file="events/001-task-a-a1.jsonl",
                    response_file="responses/001-task-a-a1.md",
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-old",
                )
                store.mark_turn_started(
                    task.task_id,
                    1,
                    thread_id="thread-old",
                    turn_id="turn-old",
                )
                with self.assertRaisesRegex(ValueError, "仅允许可安全重跑状态"):
                    store.set_pending(task.task_id)
                self.assertEqual(store.task(task.task_id)["status"], "RUNNING")

            world = FakeWorld()
            error_output = io.StringIO()
            with contextlib.redirect_stderr(error_output):
                result = execute_batch(
                    settings,
                    [task],
                    rerun=[task.task_id],
                    sdk_api=fake_sdk(world),
                )
            self.assertEqual(result, 2)
            self.assertEqual(world.turn_ids, [])
            self.assertIn("禁止使用 --rerun", error_output.getvalue())
            with self.assertRaisesRegex(ValueError, "不能使用 --retry-incomplete"):
                execute_batch(
                    settings,
                    [task],
                    retry_incomplete=[task.task_id],
                    sdk_api=fake_sdk(world),
                )
            self.assertEqual(world.turn_request_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task(task.task_id)["status"], "RUNNING")
                attempts = list(
                    store.conn.execute(
                        "SELECT attempt_no, status, turn_id FROM attempts "
                        "WHERE task_id=? ORDER BY attempt_no",
                        (task.task_id,),
                    )
                )
                self.assertEqual(
                    [tuple(row) for row in attempts],
                    [(1, "RUNNING", "turn-old")],
                )

    def test_rerun_detects_unresolved_attempt_behind_terminal_task(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="旧状态保护测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-task-a-a1.txt",
                    event_file="events/001-task-a-a1.jsonl",
                    response_file="responses/001-task-a-a1.md",
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-old",
                )
                store.mark_turn_started(
                    task.task_id,
                    1,
                    thread_id="thread-old",
                    turn_id="turn-old",
                )
                store.conn.execute(
                    "UPDATE tasks SET status='SUCCEEDED' WHERE task_id=?",
                    (task.task_id,),
                )
                store.conn.commit()
                with self.assertRaisesRegex(ValueError, "存在未完成 Attempt"):
                    store.set_pending(task.task_id)

            world = FakeWorld()
            error_output = io.StringIO()
            with contextlib.redirect_stderr(error_output):
                result = execute_batch(
                    settings,
                    [task],
                    rerun=[task.task_id],
                    sdk_api=fake_sdk(world),
                )
            self.assertEqual(result, 2)
            self.assertEqual(world.turn_ids, [])
            self.assertIn("task-a/a1:RUNNING", error_output.getvalue())

    def test_continue_on_error_stops_after_incomplete_turn(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="未知状态停止测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            world = FakeWorld()
            world.stream_error_turns.add(1)

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = execute_batch(settings, tasks, sdk_api=fake_sdk(world))

            self.assertEqual(result, 1)
            self.assertEqual(world.turn_ids, ["turn-1"])
            self.assertIn("无论 continue_on_error", output.getvalue())
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                self.assertEqual(store.latest_attempt("task-a")["status"], "INCOMPLETE")
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

    def test_run_one_task_does_not_reclassify_sqlite_checkpoint_errors(
        self,
    ) -> None:
        cases = (
            ("adopt_thread_id", "TURN_NOT_REQUESTED", None, 0),
            ("mark_turn_start_requested", "TURN_NOT_REQUESTED", None, 0),
            ("mark_turn_started", "TURN_START_REQUESTED", "thread-shared", 1),
        )
        for method_name, expected_status, expected_thread_id, turn_requests in cases:
            with self.subTest(method=method_name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                (project_dir / "AGENTS.md").write_text(
                    "# Rules\n",
                    encoding="utf-8",
                )
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="SQLite 检查点异常测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=True,
                )
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                world = FakeWorld()
                primary_error = sqlite3.OperationalError("database is locked")
                primary_error.sqlite_errorcode = 5
                secondary_error = sqlite3.OperationalError(
                    "secondary checkpoint write failed"
                )
                secondary_error.sqlite_errorcode = 5

                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    store.set_meta("project_dir", str(project_dir))
                    with mock.patch.object(
                        Store,
                        method_name,
                        side_effect=primary_error,
                    ), mock.patch.object(
                        Store,
                        "finish_attempt",
                        side_effect=secondary_error,
                    ) as finish_attempt, contextlib.redirect_stdout(io.StringIO()):
                        with self.assertRaises(sqlite3.OperationalError) as raised:
                            run_one_task(
                                store,
                                settings,
                                task,
                                sdk_api=fake_sdk(world),
                                stream_events=True,
                            )

                    self.assertIs(raised.exception, primary_error)
                    finish_attempt.assert_not_called()
                    task_row = store.task(task.task_id)
                    attempt = store.latest_attempt(task.task_id)
                    self.assertEqual(task_row["status"], expected_status)
                    self.assertEqual(attempt["status"], expected_status)
                    self.assertEqual(attempt["thread_id"], expected_thread_id)
                    self.assertIsNone(attempt["turn_id"])
                    self.assertEqual(world.turn_request_count, turn_requests)

    def test_start_attempt_uses_atomic_task_source_cas(self) -> None:
        for field, conflicting_value in (
            ("status", "INCOMPLETE"),
            ("enabled", 0),
            ("fingerprint", "external-fingerprint"),
            ("latest_attempt", 7),
        ):
            with self.subTest(
                field=field
            ), tempfile.TemporaryDirectory() as directory:
                db_path = Path(directory) / "run.sqlite3"
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                with Store(db_path) as store:
                    store.sync_tasks([task])
                    with store.conn:
                        store.conn.execute(
                            f"UPDATE tasks SET {field}=? WHERE task_id=?",
                            (conflicting_value, task.task_id),
                        )
                    task_before = dict(store.task(task.task_id))

                    with self.assertRaises(TurnCheckpointConflict):
                        store.start_attempt(
                            task,
                            attempt_no=1,
                            prompt="must not start",
                            agents_snapshot=[],
                            prompt_file="prompts/task-a.txt",
                            event_file="events/task-a.jsonl",
                            response_file="responses/task-a.md",
                        )

                    self.assertFalse(store.conn.in_transaction)
                    self.assertEqual(
                        dict(store.task(task.task_id)),
                        task_before,
                    )
                    attempt_count = store.conn.execute(
                        "SELECT COUNT(*) FROM attempts WHERE task_id=?",
                        (task.task_id,),
                    ).fetchone()[0]
                    self.assertEqual(attempt_count, 0)

                with Store(db_path) as store:
                    self.assertEqual(
                        dict(store.task(task.task_id)),
                        task_before,
                    )
                    attempt_count = store.conn.execute(
                        "SELECT COUNT(*) FROM attempts WHERE task_id=?",
                        (task.task_id,),
                    ).fetchone()[0]
                    self.assertEqual(attempt_count, 0)

        with self.subTest(
            field="TaskSpec.enabled"
        ), tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            stored_task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            disabled_task = TaskSpec("task-a", 1, False, "A.md", "A()", "A.c")
            with Store(db_path) as store:
                store.sync_tasks([stored_task])
                task_before = dict(store.task(stored_task.task_id))

                with self.assertRaises(TurnCheckpointConflict):
                    store.start_attempt(
                        disabled_task,
                        attempt_no=1,
                        prompt="must not start",
                        agents_snapshot=[],
                        prompt_file="prompts/task-a.txt",
                        event_file="events/task-a.jsonl",
                        response_file="responses/task-a.md",
                    )

                self.assertFalse(store.conn.in_transaction)
                self.assertEqual(
                    dict(store.task(stored_task.task_id)),
                    task_before,
                )
                attempt_count = store.conn.execute(
                    "SELECT COUNT(*) FROM attempts WHERE task_id=?",
                    (stored_task.task_id,),
                ).fetchone()[0]
                self.assertEqual(attempt_count, 0)

    def test_start_attempt_rolls_back_task_cas_when_insert_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            with Store(db_path) as store:
                store.sync_tasks([task])
                task_before = dict(store.task(task.task_id))
                with store.conn:
                    store.conn.execute(
                        """
                        CREATE TEMP TRIGGER fail_attempt_insert
                        BEFORE INSERT ON attempts
                        BEGIN
                            SELECT RAISE(ABORT, 'injected attempt insert failure');
                        END
                        """
                    )

                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "injected attempt insert failure",
                ):
                    store.start_attempt(
                        task,
                        attempt_no=1,
                        prompt="must roll back",
                        agents_snapshot=[],
                        prompt_file="prompts/task-a.txt",
                        event_file="events/task-a.jsonl",
                        response_file="responses/task-a.md",
                    )

                self.assertFalse(store.conn.in_transaction)
                self.assertEqual(dict(store.task(task.task_id)), task_before)
                attempt_count = store.conn.execute(
                    "SELECT COUNT(*) FROM attempts WHERE task_id=?",
                    (task.task_id,),
                ).fetchone()[0]
                self.assertEqual(attempt_count, 0)

    def test_turn_checkpoint_cas_conflicts_are_typed_and_atomic(self) -> None:
        cases = (
            ("mark_turn_start_requested", "TURN_NOT_REQUESTED", None),
            ("mark_turn_started", "TURN_START_REQUESTED", "thread-shared"),
        )
        for method_name, expected_attempt_status, expected_thread_id in cases:
            with self.subTest(
                method=method_name
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                with Store(root / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    store.start_attempt(
                        task,
                        attempt_no=1,
                        prompt="test prompt",
                        agents_snapshot=[],
                        prompt_file="prompts/task-a.txt",
                        event_file="events/task-a.jsonl",
                        response_file="responses/task-a.md",
                    )
                    if method_name == "mark_turn_started":
                        store.mark_turn_start_requested(
                            task.task_id,
                            1,
                            thread_id="thread-shared",
                        )

                    # 让第二条 task CAS 失败；第一条 attempt UPDATE 必须随异常回滚。
                    with store.conn:
                        store.conn.execute(
                            "UPDATE tasks SET status='INCOMPLETE' WHERE task_id=?",
                            (task.task_id,),
                        )

                    kwargs = {"thread_id": "thread-shared"}
                    if method_name == "mark_turn_started":
                        kwargs["turn_id"] = "turn-1"
                    with self.assertRaises(TurnCheckpointConflict):
                        getattr(store, method_name)(task.task_id, 1, **kwargs)

                    self.assertEqual(
                        store.task(task.task_id)["status"],
                        "INCOMPLETE",
                    )
                    attempt = store.latest_attempt(task.task_id)
                    self.assertEqual(attempt["status"], expected_attempt_status)
                    self.assertEqual(attempt["thread_id"], expected_thread_id)
                    self.assertIsNone(attempt["turn_id"])

    def test_mark_turn_started_cannot_overwrite_existing_turn_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            with Store(root / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt="turn identity conflict",
                    agents_snapshot=[],
                    prompt_file="prompts/task-a.txt",
                    event_file="events/task-a.jsonl",
                    response_file="responses/task-a.md",
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                )
                with store.conn:
                    store.conn.execute(
                        "UPDATE attempts SET turn_id='turn-external' "
                        "WHERE task_id=? AND attempt_no=?",
                        (task.task_id, 1),
                    )

                with self.assertRaises(TurnCheckpointConflict):
                    store.mark_turn_started(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                        turn_id="turn-1",
                    )

                self.assertEqual(
                    store.task(task.task_id)["status"],
                    "TURN_START_REQUESTED",
                )
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "TURN_START_REQUESTED")
                self.assertEqual(attempt["turn_id"], "turn-external")

    def test_mark_turn_started_rejects_invalid_turn_id_atomically(self) -> None:
        for raw_turn_id in (None, "", " \t"):
            with self.subTest(raw_turn_id=raw_turn_id), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                with Store(root / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    store.start_attempt(
                        task,
                        attempt_no=1,
                        prompt="invalid turn identity",
                        agents_snapshot=[],
                        prompt_file="prompts/task-a.txt",
                        event_file="events/task-a.jsonl",
                        response_file="responses/task-a.md",
                    )
                    store.mark_turn_start_requested(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                    )

                    with self.assertRaises(TurnIdentityError):
                        store.mark_turn_started(
                            task.task_id,
                            1,
                            thread_id="thread-shared",
                            turn_id=raw_turn_id,
                        )

                    self.assertEqual(
                        store.task(task.task_id)["status"],
                        "TURN_START_REQUESTED",
                    )
                    attempt = store.latest_attempt(task.task_id)
                    self.assertEqual(
                        attempt["status"],
                        "TURN_START_REQUESTED",
                    )
                    self.assertIsNone(attempt["turn_id"])

    def test_batch_does_not_reclassify_turn_checkpoint_cas_conflicts(
        self,
    ) -> None:
        cases = (
            ("mark_turn_start_requested", "TURN_START_REQUESTED", None, 0),
            ("mark_turn_started", "RUNNING", "turn-external", 1),
        )
        for (
            method_name,
            expected_status,
            expected_turn_id,
            turn_requests,
        ) in cases:
            with self.subTest(
                method=method_name
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                (project_dir / "AGENTS.md").write_text(
                    "# Rules\n",
                    encoding="utf-8",
                )
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="检查点 CAS 冲突测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=True,
                )
                tasks = [
                    TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                    TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
                ]
                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    store.sync_tasks(tasks)
                    store.set_meta("project_dir", str(project_dir))
                    store.set_meta("thread_id", "thread-shared")

                real_checkpoint = getattr(Store, method_name)
                real_finish_attempt = Store.finish_attempt
                finish_calls: list[str] = []

                def inject_conflict(
                    store: Store,
                    task_id: str,
                    attempt_no: int,
                    **kwargs,
                ) -> None:
                    if task_id == "task-a":
                        with store.conn:
                            if method_name == "mark_turn_start_requested":
                                store.conn.execute(
                                    """
                                    UPDATE attempts
                                    SET thread_id='thread-shared',
                                        status='TURN_START_REQUESTED'
                                    WHERE task_id=? AND attempt_no=?
                                    """,
                                    (task_id, attempt_no),
                                )
                            else:
                                store.conn.execute(
                                    """
                                    UPDATE attempts
                                    SET thread_id='thread-shared',
                                        turn_id='turn-external', status='RUNNING'
                                    WHERE task_id=? AND attempt_no=?
                                    """,
                                    (task_id, attempt_no),
                                )
                            store.conn.execute(
                                "UPDATE tasks SET status=? WHERE task_id=?",
                                (expected_status, task_id),
                            )
                    real_checkpoint(store, task_id, attempt_no, **kwargs)

                def track_finish_attempt(store: Store, *args, **kwargs) -> None:
                    finish_calls.append(str(args[0]))
                    real_finish_attempt(store, *args, **kwargs)

                world = FakeWorld()
                with mock.patch.object(
                    Store,
                    method_name,
                    new=inject_conflict,
                ), mock.patch.object(
                    Store,
                    "finish_attempt",
                    new=track_finish_attempt,
                ), contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(TurnCheckpointConflict):
                        execute_batch(settings, tasks, sdk_api=fake_sdk(world))

                self.assertEqual(finish_calls, [])
                self.assertEqual(world.turn_request_count, turn_requests)
                self.assertEqual(world.remote_accept_count, turn_requests)
                with Store(output_dir / "run.sqlite3") as store:
                    self.assertEqual(
                        store.task("task-a")["status"],
                        expected_status,
                    )
                    self.assertEqual(
                        store.latest_attempt("task-a")["status"],
                        expected_status,
                    )
                    self.assertEqual(
                        store.latest_attempt("task-a")["thread_id"],
                        "thread-shared",
                    )
                    self.assertEqual(
                        store.latest_attempt("task-a")["turn_id"],
                        expected_turn_id,
                    )
                    self.assertEqual(store.task("task-b")["status"], "PENDING")
                    self.assertIsNone(store.latest_attempt("task-b"))

    def test_finish_attempt_uses_atomic_source_and_latest_attempt_cas(
        self,
    ) -> None:
        for conflict in (
            "attempt_status",
            "task_status",
            "latest_attempt",
            "turn_id",
        ):
            with self.subTest(
                conflict=conflict
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                with Store(root / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    store.start_attempt(
                        task,
                        attempt_no=1,
                        prompt="attempt 1",
                        agents_snapshot=[],
                        prompt_file="prompts/task-a-a1.txt",
                        event_file="events/task-a-a1.jsonl",
                        response_file="responses/task-a-a1.md",
                    )
                    store.mark_turn_start_requested(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                    )
                    store.mark_turn_started(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                        turn_id="turn-1",
                    )

                    if conflict == "attempt_status":
                        with store.conn:
                            store.conn.execute(
                                """
                                UPDATE attempts SET status='INCOMPLETE'
                                WHERE task_id=? AND attempt_no=1
                                """,
                                (task.task_id,),
                            )
                    elif conflict == "task_status":
                        with store.conn:
                            store.conn.execute(
                                """
                                UPDATE tasks SET status='INCOMPLETE'
                                WHERE task_id=?
                                """,
                                (task.task_id,),
                            )
                    elif conflict == "latest_attempt":
                        # 直接注入竞争快照：公开 start_attempt() 不应允许从
                        # RUNNING 启动后续 Attempt。
                        with store.conn:
                            store.conn.execute(
                                """
                                INSERT INTO attempts(
                                    task_id, attempt_no, status, started_at,
                                    prompt, agents_snapshot_json, prompt_file,
                                    event_file, response_file
                                ) VALUES(
                                    ?, 2, 'TURN_NOT_REQUESTED', ?, ?, '[]', ?, ?, ?
                                )
                                """,
                                (
                                    task.task_id,
                                    "injected-started-at",
                                    "attempt 2",
                                    "prompts/task-a-a2.txt",
                                    "events/task-a-a2.jsonl",
                                    "responses/task-a-a2.md",
                                ),
                            )
                            store.conn.execute(
                                """
                                UPDATE tasks
                                SET status='RUNNING', latest_attempt=2
                                WHERE task_id=?
                                """,
                                (task.task_id,),
                            )
                    else:
                        with store.conn:
                            store.conn.execute(
                                "UPDATE attempts SET turn_id='turn-external' "
                                "WHERE task_id=? AND attempt_no=?",
                                (task.task_id, 1),
                            )

                    with self.assertRaises(TurnCheckpointConflict):
                        store.finish_attempt(
                            task.task_id,
                            1,
                            expected_task_status="RUNNING",
                            expected_attempt_status="RUNNING",
                            expected_turn_id="turn-1",
                            task_status="SUCCEEDED",
                            attempt_status="SUCCEEDED",
                            final_response="must not commit",
                            error=None,
                            usage=None,
                            duration_ms=1,
                        )

                    attempt_1 = store.conn.execute(
                        """
                        SELECT * FROM attempts
                        WHERE task_id=? AND attempt_no=1
                        """,
                        (task.task_id,),
                    ).fetchone()
                    self.assertFalse(store.conn.in_transaction)
                    self.assertIsNone(attempt_1["completed_at"])
                    self.assertIsNone(attempt_1["final_response"])
                    if conflict == "attempt_status":
                        self.assertEqual(store.task(task.task_id)["status"], "RUNNING")
                        self.assertEqual(attempt_1["status"], "INCOMPLETE")
                    elif conflict == "task_status":
                        self.assertEqual(
                            store.task(task.task_id)["status"],
                            "INCOMPLETE",
                        )
                        self.assertEqual(attempt_1["status"], "RUNNING")
                    elif conflict == "latest_attempt":
                        self.assertEqual(
                            store.task(task.task_id)["status"],
                            "RUNNING",
                        )
                        self.assertEqual(
                            store.task(task.task_id)["latest_attempt"],
                            2,
                        )
                        self.assertEqual(attempt_1["status"], "RUNNING")
                        self.assertEqual(
                            store.latest_attempt(task.task_id)["status"],
                            "TURN_NOT_REQUESTED",
                        )
                    else:
                        self.assertEqual(
                            store.task(task.task_id)["status"],
                            "RUNNING",
                        )
                        self.assertEqual(attempt_1["status"], "RUNNING")
                        self.assertEqual(
                            attempt_1["turn_id"],
                            "turn-external",
                        )

    def test_finish_attempt_rolls_back_when_response_publish_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            with Store(root / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt="publish failure",
                    agents_snapshot=[],
                    prompt_file="prompts/task-a.txt",
                    event_file="events/task-a.jsonl",
                    response_file="responses/task-a.md",
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                )
                store.mark_turn_started(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                    turn_id="turn-1",
                )
                observed: list[tuple[bool, str, str]] = []

                def fail_publish() -> None:
                    observed.append(
                        (
                            store.conn.in_transaction,
                            store.task(task.task_id)["status"],
                            store.latest_attempt(task.task_id)["status"],
                        )
                    )
                    raise OSError("injected response publish failure")

                with self.assertRaisesRegex(
                    OSError,
                    "injected response publish failure",
                ):
                    store.finish_attempt(
                        task.task_id,
                        1,
                        expected_task_status="RUNNING",
                        expected_attempt_status="RUNNING",
                        expected_turn_id="turn-1",
                        task_status="SUCCEEDED",
                        attempt_status="SUCCEEDED",
                        final_response="must roll back",
                        error={"message": "must roll back"},
                        usage={"tokens": 1},
                        duration_ms=1,
                        publish_response=fail_publish,
                    )

                self.assertEqual(observed, [(True, "SUCCEEDED", "SUCCEEDED")])
                self.assertFalse(store.conn.in_transaction)
                self.assertEqual(store.task(task.task_id)["status"], "RUNNING")
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "RUNNING")
                self.assertIsNone(attempt["completed_at"])
                self.assertIsNone(attempt["final_response"])
                self.assertIsNone(attempt["error_json"])
                self.assertIsNone(attempt["usage_json"])

    def test_batch_stops_when_finish_attempt_cas_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="Attempt 收尾 CAS 冲突测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            real_finish_attempt = Store.finish_attempt
            ensure_secure_output_directory(settings)
            response_relative = self._attempt_artifact_relatives("task-a", 1)[2]
            response_path = output_dir / response_relative
            atomic_write_text(
                output_dir,
                response_relative,
                "EXISTING RESPONSE\n",
                field="response_file",
                expected_directory="responses",
            )

            def inject_conflict(
                store: Store,
                task_id: str,
                attempt_no: int,
                **kwargs,
            ) -> None:
                if task_id == "task-a":
                    with store.conn:
                        store.conn.execute(
                            "UPDATE attempts SET status='INCOMPLETE' "
                            "WHERE task_id=? AND attempt_no=?",
                            (task_id, attempt_no),
                        )
                        store.conn.execute(
                            "UPDATE tasks SET status='INCOMPLETE' WHERE task_id=?",
                            (task_id,),
                        )
                real_finish_attempt(store, task_id, attempt_no, **kwargs)

            world = FakeWorld()
            with mock.patch.object(
                Store,
                "finish_attempt",
                new=inject_conflict,
            ), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(TurnCheckpointConflict):
                    execute_batch(settings, tasks, sdk_api=fake_sdk(world))

            self.assertEqual(world.turn_ids, ["turn-1"])
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            self.assertEqual(
                response_path.read_text(encoding="utf-8"),
                "EXISTING RESPONSE\n",
            )
            self.assertEqual(
                [path.name for path in response_path.parent.iterdir()],
                [response_path.name],
            )
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                attempt = store.latest_attempt("task-a")
                self.assertEqual(attempt["status"], "INCOMPLETE")
                self.assertIsNone(attempt["completed_at"])
                self.assertIsNone(attempt["final_response"])
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

    def test_live_response_publish_failure_rolls_back_and_removes_candidate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="响应发布失败测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            response_path = (
                output_dir / self._attempt_artifact_relatives("task-a", 1)[2]
            )
            from codex_batch import replace_descriptor_path

            def fail_response_replace(
                source: str,
                destination: str,
                *,
                source_dir_fd: int,
                destination_dir_fd: int,
            ) -> None:
                if destination == response_path.name:
                    raise OSError("injected response replace failure")
                replace_descriptor_path(
                    source,
                    destination,
                    source_dir_fd=source_dir_fd,
                    destination_dir_fd=destination_dir_fd,
                )

            world = FakeWorld()
            with mock.patch(
                "codex_batch.replace_descriptor_path",
                new=fail_response_replace,
            ), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(
                    OSError,
                    "injected response replace failure",
                ):
                    execute_batch(settings, tasks, sdk_api=fake_sdk(world))

            self.assertEqual(world.turn_ids, ["turn-1"])
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            self.assertFalse(response_path.exists())
            self.assertEqual(list(response_path.parent.iterdir()), [])
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "RUNNING")
                attempt = store.latest_attempt("task-a")
                self.assertEqual(attempt["status"], "RUNNING")
                self.assertIsNone(attempt["completed_at"])
                self.assertIsNone(attempt["final_response"])
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

    def test_finish_attempt_accepts_distinct_exact_source_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            with Store(root / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt="recovery snapshot",
                    agents_snapshot=[],
                    prompt_file="prompts/task-a.txt",
                    event_file="events/task-a.jsonl",
                    response_file="responses/task-a.md",
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                )
                store.mark_turn_started(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                    turn_id="turn-1",
                )
                with store.conn:
                    store.conn.execute(
                        "UPDATE tasks SET status='INCOMPLETE' WHERE task_id=?",
                        (task.task_id,),
                    )

                store.finish_attempt(
                    task.task_id,
                    1,
                    expected_task_status="INCOMPLETE",
                    expected_attempt_status="RUNNING",
                    expected_turn_id="turn-1",
                    task_status="SUCCEEDED",
                    attempt_status="SUCCEEDED",
                    final_response="recovered",
                    error=None,
                    usage=None,
                    duration_ms=1,
                )

                self.assertEqual(store.task(task.task_id)["status"], "SUCCEEDED")
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "SUCCEEDED")
                self.assertEqual(attempt["final_response"], "recovered")

    def test_keyboard_interrupt_finishes_from_committed_checkpoint(self) -> None:
        cases = (
            ("before_request", None, None, 0),
            ("start_requested", "thread-shared", None, 1),
            ("running", "thread-shared", "turn-1", 1),
        )
        for stage, expected_thread_id, expected_turn_id, turn_requests in cases:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                (project_dir / "AGENTS.md").write_text(
                    "# Rules\n",
                    encoding="utf-8",
                )
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="中断检查点测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=False,
                )
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    store.set_meta("project_dir", str(project_dir))
                    store.set_meta("thread_id", "thread-shared")

                world = FakeWorld()

                def interrupt_turn(_thread, _prompt, **_kwargs):
                    world.turn_request_count += 1
                    world.remote_accept_count += 1
                    raise KeyboardInterrupt

                with contextlib.ExitStack() as stack:
                    if stage == "before_request":
                        stack.enter_context(
                            mock.patch(
                                "codex_batch.turn_kwargs",
                                side_effect=KeyboardInterrupt,
                            )
                        )
                    elif stage == "start_requested":
                        stack.enter_context(
                            mock.patch.object(
                                FakeThread,
                                "turn",
                                new=interrupt_turn,
                            )
                        )
                    else:
                        stack.enter_context(
                            mock.patch(
                                "codex_batch.collect_stream",
                                side_effect=KeyboardInterrupt,
                            )
                        )
                    stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                    with self.assertRaises(KeyboardInterrupt):
                        execute_batch(
                            settings,
                            [task],
                            sdk_api=fake_sdk(world),
                        )

                self.assertEqual(world.turn_request_count, turn_requests)
                self.assertEqual(world.remote_accept_count, turn_requests)
                with Store(output_dir / "run.sqlite3") as store:
                    self.assertEqual(store.task(task.task_id)["status"], "INCOMPLETE")
                    attempt = store.latest_attempt(task.task_id)
                    self.assertEqual(attempt["status"], "INCOMPLETE")
                    self.assertEqual(attempt["thread_id"], expected_thread_id)
                    self.assertEqual(attempt["turn_id"], expected_turn_id)
                    self.assertIsNotNone(attempt["completed_at"])

    def test_continue_on_error_allows_known_terminal_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="明确失败继续测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            world = FakeWorld()
            world.terminal_failure_turns.add(1)

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = execute_batch(settings, tasks, sdk_api=fake_sdk(world))

            self.assertEqual(result, 1)
            self.assertEqual(world.turn_ids, ["turn-1", "turn-2"])
            self.assertNotIn("状态未知或未完成", output.getvalue())
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(
                    store.task("task-a")["status"],
                    "FAILED_TERMINAL",
                )
                failed_attempt = store.latest_attempt("task-a")
                self.assertEqual(failed_attempt["status"], "FAILED_TERMINAL")
                self.assertEqual(failed_attempt["turn_id"], "turn-1")
                self.assertEqual(store.task("task-b")["status"], "SUCCEEDED")

            with contextlib.redirect_stdout(io.StringIO()):
                retry_result = execute_batch(
                    settings,
                    tasks,
                    sdk_api=fake_sdk(world),
                )
            self.assertEqual(retry_result, 0)
            self.assertEqual(world.turn_ids, ["turn-1", "turn-2", "turn-3"])
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "SUCCEEDED")
                self.assertEqual(store.latest_attempt("task-a")["attempt_no"], 2)

    def test_continue_on_error_stops_for_unrecognized_completed_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="未知终态测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            world = FakeWorld()
            world.unknown_status_turns.add(1)

            with contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(settings, tasks, sdk_api=fake_sdk(world))

            self.assertEqual(result, 1)
            self.assertEqual(world.turn_ids, ["turn-1"])
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                self.assertEqual(store.task("task-b")["status"], "PENDING")

    def test_turn_start_response_loss_is_not_automatically_retried(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="Turn 启动响应丢失测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            world = FakeWorld()
            world.lost_turn_start_responses.add(1)

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                first_result = execute_batch(
                    settings,
                    tasks,
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(first_result, 1)
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            self.assertEqual(world.turn_ids, [])
            self.assertIn("无论 continue_on_error", output.getvalue())
            with Store(output_dir / "run.sqlite3") as store:
                first_attempt = store.latest_attempt("task-a")
                self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                self.assertEqual(first_attempt["status"], "INCOMPLETE")
                self.assertEqual(first_attempt["thread_id"], "thread-shared")
                self.assertIsNone(first_attempt["turn_id"])
                self.assertIn("启动响应丢失", first_attempt["error_json"])
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

            error_output = io.StringIO()
            with contextlib.redirect_stderr(error_output):
                second_result = execute_batch(
                    settings,
                    tasks,
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(second_result, 2)
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            self.assertIn("task-a/a1:INCOMPLETE", error_output.getvalue())

            with self.assertRaisesRegex(ValueError, "不能使用 --retry-incomplete"):
                execute_batch(
                    settings,
                    tasks,
                    retry_incomplete=["task-a"],
                    sdk_api=fake_sdk(world),
                )
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            with Store(output_dir / "run.sqlite3") as store:
                attempts = list(
                    store.conn.execute(
                        "SELECT attempt_no, status FROM attempts "
                        "WHERE task_id=? ORDER BY attempt_no",
                        ("task-a",),
                    )
                )
                self.assertEqual([tuple(row) for row in attempts], [(1, "INCOMPLETE")])

    def test_rejects_missing_or_blank_handle_turn_id_after_request(self) -> None:
        for raw_turn_id in (None, "", " \t"):
            with self.subTest(raw_turn_id=raw_turn_id), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                settings, tasks = self._turn_identity_fixture(
                    root,
                    thread_name="Handle Turn ID 身份测试",
                )
                world = FakeWorld()
                world.handle_turn_id_overrides[1] = raw_turn_id

                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    result = execute_batch(
                        settings,
                        tasks,
                        sdk_api=fake_sdk(world),
                    )

                self.assertEqual(result, 1)
                self.assertEqual(world.turn_request_count, 1)
                self.assertEqual(world.remote_accept_count, 1)
                self.assertEqual(world.stream_count, 0)
                self.assertEqual(world.run_count, 0)
                self.assertEqual(world.turn_ids, ["turn-1"])
                self.assertIn("无论 continue_on_error", output.getvalue())
                self.assertFalse(
                    (
                        settings.output_dir
                        / self._attempt_artifact_relatives("task-a", 1)[2]
                    ).exists()
                )
                with Store(settings.output_dir / "run.sqlite3") as store:
                    self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                    attempt = store.latest_attempt("task-a")
                    self.assertEqual(attempt["status"], "INCOMPLETE")
                    self.assertEqual(attempt["thread_id"], "thread-shared")
                    self.assertIsNone(attempt["turn_id"])
                    self.assertIsNotNone(attempt["completed_at"])
                    self.assertIsNone(attempt["final_response"])
                    error = json.loads(attempt["error_json"])
                    self.assertEqual(error["type"], "TurnIdentityError")
                    self.assertIn("handle.id", error["message"])
                    self.assertIn("缺少有效", error["message"])
                    self.assertEqual(store.task("task-b")["status"], "PENDING")
                    self.assertIsNone(store.latest_attempt("task-b"))

    def test_stream_rejects_missing_or_mismatched_turn_identity(self) -> None:
        cases = (
            ("turn/started", None),
            ("turn/started", "turn-other"),
            ("turn/completed", None),
            ("turn/completed", "turn-other"),
        )
        for source, raw_turn_id in cases:
            with self.subTest(source=source, raw_turn_id=raw_turn_id), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                settings, tasks = self._turn_identity_fixture(
                    root,
                    thread_name="流式 Turn ID 身份测试",
                )
                world = FakeWorld()
                overrides = (
                    world.started_turn_id_overrides
                    if source == "turn/started"
                    else world.completed_turn_id_overrides
                )
                overrides[1] = raw_turn_id

                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    result = execute_batch(
                        settings,
                        tasks,
                        sdk_api=fake_sdk(world),
                    )

                self.assertEqual(result, 1)
                self.assertEqual(world.turn_request_count, 1)
                self.assertEqual(world.remote_accept_count, 1)
                self.assertEqual(world.stream_count, 1)
                self.assertEqual(world.run_count, 0)
                self.assertEqual(world.turn_ids, ["turn-1"])
                self.assertIn("无论 continue_on_error", output.getvalue())
                self.assertFalse(
                    (
                        settings.output_dir
                        / self._attempt_artifact_relatives("task-a", 1)[2]
                    ).exists()
                )
                with Store(settings.output_dir / "run.sqlite3") as store:
                    self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                    attempt = store.latest_attempt("task-a")
                    self.assertEqual(attempt["status"], "INCOMPLETE")
                    self.assertEqual(attempt["thread_id"], "thread-shared")
                    self.assertEqual(attempt["turn_id"], "turn-1")
                    self.assertIsNotNone(attempt["completed_at"])
                    self.assertIsNone(attempt["final_response"])
                    error = json.loads(attempt["error_json"])
                    self.assertEqual(error["type"], "TurnIdentityError")
                    self.assertIn(source, error["message"])
                    self.assertIn("turn-1", error["message"])
                    if raw_turn_id is None:
                        self.assertIn("缺少有效", error["message"])
                    else:
                        self.assertIn("不一致", error["message"])
                        self.assertIn("turn-other", error["message"])
                    self.assertEqual(store.task("task-b")["status"], "PENDING")
                    self.assertIsNone(store.latest_attempt("task-b"))

    def test_rejected_stream_identity_cannot_be_recovered_later(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings, tasks = self._turn_identity_fixture(
                root,
                thread_name="流式 Turn ID 恢复边界测试",
            )
            world = FakeWorld()
            world.completed_turn_id_omissions.add(1)

            with contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(
                    settings,
                    tasks,
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 1)
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            with Store(settings.output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                self.assertEqual(
                    store.latest_attempt("task-a")["status"],
                    "INCOMPLETE",
                )

            with prepare_store(settings, tasks) as store:
                self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                attempt = store.latest_attempt("task-a")
                self.assertEqual(attempt["status"], "INCOMPLETE")
                self.assertEqual(attempt["turn_id"], "turn-1")
                self.assertIsNone(attempt["final_response"])
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

            self.assertEqual(world.turn_request_count, 1)
            self.assertFalse(
                (
                    settings.output_dir
                    / self._attempt_artifact_relatives("task-a", 1)[2]
                ).exists()
            )

    def test_nonstream_rejects_missing_or_mismatched_result_turn_identity(
        self,
    ) -> None:
        for raw_turn_id in (None, "turn-other"):
            with self.subTest(raw_turn_id=raw_turn_id), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                settings, tasks = self._turn_identity_fixture(
                    root,
                    thread_name="非流式 Turn ID 身份测试",
                )
                world = FakeWorld()
                world.run_result_turn_id_overrides[1] = raw_turn_id

                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    result = execute_batch(
                        settings,
                        tasks,
                        no_stream=True,
                        sdk_api=fake_sdk(world),
                    )

                self.assertEqual(result, 1)
                self.assertEqual(world.turn_request_count, 1)
                self.assertEqual(world.remote_accept_count, 1)
                self.assertEqual(world.stream_count, 0)
                self.assertEqual(world.run_count, 1)
                self.assertEqual(world.turn_ids, ["turn-1"])
                self.assertIn("无论 continue_on_error", output.getvalue())
                artifact_paths = self._attempt_artifact_relatives("task-a", 1)
                self.assertFalse(
                    (settings.output_dir / artifact_paths[2]).exists()
                )
                event_record = json.loads(
                    (settings.output_dir / artifact_paths[1]).read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(event_record["payload"]["turn_id"], "turn-1")
                with Store(settings.output_dir / "run.sqlite3") as store:
                    self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                    attempt = store.latest_attempt("task-a")
                    self.assertEqual(attempt["status"], "INCOMPLETE")
                    self.assertEqual(attempt["thread_id"], "thread-shared")
                    self.assertEqual(attempt["turn_id"], "turn-1")
                    self.assertIsNotNone(attempt["completed_at"])
                    self.assertIsNone(attempt["final_response"])
                    error = json.loads(attempt["error_json"])
                    self.assertEqual(error["type"], "TurnIdentityError")
                    self.assertIn("handle.run() result.id", error["message"])
                    self.assertIn("turn-1", error["message"])
                    if raw_turn_id is None:
                        self.assertIn("缺少有效", error["message"])
                    else:
                        self.assertIn("不一致", error["message"])
                        self.assertIn("turn-other", error["message"])
                    self.assertEqual(store.task("task-b")["status"], "PENDING")
                    self.assertIsNone(store.latest_attempt("task-b"))

    def test_nonstream_accepts_matching_result_turn_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings, tasks = self._turn_identity_fixture(
                root,
                thread_name="非流式 Turn ID 成功测试",
            )
            world = FakeWorld()

            with contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(
                    settings,
                    tasks,
                    limit=1,
                    no_stream=True,
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 0)
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            self.assertEqual(world.stream_count, 0)
            self.assertEqual(world.run_count, 1)
            self.assertEqual(world.turn_ids, ["turn-1"])
            artifact_paths = self._attempt_artifact_relatives("task-a", 1)
            response_path = settings.output_dir / artifact_paths[2]
            self.assertTrue(response_path.is_file())
            event_record = json.loads(
                (settings.output_dir / artifact_paths[1]).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(event_record["payload"]["turn_id"], "turn-1")
            with Store(settings.output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "SUCCEEDED")
                attempt = store.latest_attempt("task-a")
                self.assertEqual(attempt["status"], "SUCCEEDED")
                self.assertEqual(attempt["turn_id"], "turn-1")
                self.assertIn("已完成 turn-1", attempt["final_response"])
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

    def test_turn_start_requested_checkpoint_blocks_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="Turn 启动请求检查点测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-task-a-a1.txt",
                    event_file="events/001-task-a-a1.jsonl",
                    response_file="responses/001-task-a-a1.md",
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                )

            world = FakeWorld()
            error_output = io.StringIO()
            with contextlib.redirect_stderr(error_output):
                result = execute_batch(
                    settings,
                    [task],
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 2)
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            self.assertIn(
                "task-a/a1:TURN_START_REQUESTED",
                error_output.getvalue(),
            )
            with self.assertRaisesRegex(ValueError, "不能使用 --retry-incomplete"):
                execute_batch(
                    settings,
                    [task],
                    retry_incomplete=[task.task_id],
                    sdk_api=fake_sdk(world),
                )
            self.assertEqual(world.turn_request_count, 0)
            with Store(output_dir / "run.sqlite3") as store:
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(
                    store.task(task.task_id)["status"],
                    "TURN_START_REQUESTED",
                )
                self.assertEqual(attempt["status"], "TURN_START_REQUESTED")
                self.assertEqual(attempt["thread_id"], "thread-shared")
                self.assertIsNone(attempt["turn_id"])

    def test_completed_event_log_recovers_interrupted_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text("# Rules\n", encoding="utf-8")
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="恢复测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("recover-me", 1, True, "A.md", "A()", "A.c")
            event_relative = "events/001-recover-me-a1.jsonl"
            response_relative = "responses/001-recover-me-a1.md"
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.set_meta("thread_id", "thread-shared")
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-recover-me-a1.txt",
                    event_file=event_relative,
                    response_file=response_relative,
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                )
                store.mark_turn_started(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                    turn_id="turn-recovered",
                )

            event_path = output_dir / event_relative
            event_path.parent.mkdir(parents=True)
            records = [
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "root": {
                                "type": "agentMessage",
                                "phase": "finalAnswer",
                                "text": "从事件日志恢复的最终回复",
                            }
                        }
                    },
                },
                {
                    "method": "turn/completed",
                    "payload": {
                        "turn": {
                            "id": "turn-recovered",
                            "status": "completed",
                            "durationMs": 1234,
                        }
                    },
                },
            ]
            event_path.write_text(
                "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in records),
                encoding="utf-8",
            )

            with prepare_store(settings, [task]) as store:
                self.assertEqual(store.task(task.task_id)["status"], "SUCCEEDED")
                self.assertEqual(
                    store.latest_attempt(task.task_id)["final_response"],
                    "从事件日志恢复的最终回复",
                )
            response = (output_dir / response_relative).read_text(encoding="utf-8")
            self.assertIn("从事件日志恢复的最终回复", response)

    def test_recovery_rejects_malformed_schema_and_untrusted_turn_identity(
        self,
    ) -> None:
        trusted_item = {
            "method": "item/completed",
            "payload": {
                "item": {
                    "root": {
                        "type": "agentMessage",
                        "phase": "finalAnswer",
                        "text": "不得采信的外部回复",
                    }
                }
            },
        }

        def terminal(
            turn: Any,
        ) -> dict[str, Any]:
            return {
                "method": "turn/completed",
                "payload": {"turn": turn},
            }

        valid_terminal = terminal(
            {"id": "turn-expected", "status": "completed"}
        )
        cases = (
            ("top-level-array", [trusted_item, [], valid_terminal], "turn-expected"),
            ("missing-method", [trusted_item, {}, valid_terminal], "turn-expected"),
            (
                "non-string-method",
                [trusted_item, {"method": 7}, valid_terminal],
                "turn-expected",
            ),
            (
                "blank-method",
                [trusted_item, {"method": "  "}, valid_terminal],
                "turn-expected",
            ),
            (
                "item-scalar-payload",
                [
                    trusted_item,
                    {"method": "item/completed", "payload": "bad"},
                    valid_terminal,
                ],
                "turn-expected",
            ),
            (
                "item-non-object",
                [
                    trusted_item,
                    {
                        "method": "item/completed",
                        "payload": {"item": []},
                    },
                    valid_terminal,
                ],
                "turn-expected",
            ),
            (
                "completed-scalar-payload",
                [
                    trusted_item,
                    {"method": "turn/completed", "payload": "bad"},
                ],
                "turn-expected",
            ),
            (
                "completed-non-object-turn",
                [trusted_item, terminal("bad")],
                "turn-expected",
            ),
            (
                "missing-id",
                [trusted_item, terminal({"status": "completed"})],
                "turn-expected",
            ),
            (
                "numeric-lookalike-id",
                [trusted_item, terminal({"id": 7, "status": "completed"})],
                "7",
            ),
            (
                "blank-id",
                [trusted_item, terminal({"id": " ", "status": "completed"})],
                "turn-expected",
            ),
            (
                "padded-id",
                [
                    trusted_item,
                    terminal(
                        {"id": " turn-expected ", "status": "completed"}
                    ),
                ],
                "turn-expected",
            ),
            (
                "mismatched-id",
                [
                    trusted_item,
                    terminal({"id": "turn-other", "status": "completed"}),
                ],
                "turn-expected",
            ),
            (
                "missing-status",
                [trusted_item, terminal({"id": "turn-expected"})],
                "turn-expected",
            ),
            (
                "non-string-status",
                [trusted_item, terminal({"id": "turn-expected", "status": 7})],
                "turn-expected",
            ),
            (
                "mismatched-started-id",
                [
                    trusted_item,
                    {
                        "method": "turn/started",
                        "payload": {"turn": {"id": "turn-other"}},
                    },
                    valid_terminal,
                ],
                "turn-expected",
            ),
            (
                "non-stream-marker-with-appended-terminal",
                [
                    trusted_item,
                    {
                        "method": "batch/nonStreamingMode",
                        "payload": {"turn_id": "turn-expected"},
                    },
                    valid_terminal,
                ],
                "turn-expected",
            ),
        )
        for case, records, turn_id in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                settings, task, event_path, response_path = self._recovery_fixture(
                    root,
                    turn_id=turn_id,
                )
                event_path.write_text(
                    "".join(
                        json.dumps(record, ensure_ascii=False) + "\n"
                        for record in records
                    ),
                    encoding="utf-8",
                )
                response_path.write_text("可信旧响应\n", encoding="utf-8")
                response_inode = response_path.stat().st_ino
                self._assert_recovery_rejected_without_state_change(
                    settings,
                    task,
                    response_path,
                    turn_id=turn_id,
                    response_inode=response_inode,
                )

    def test_recovery_rejects_duplicate_or_nonterminal_completion(self) -> None:
        item = {
            "method": "item/completed",
            "payload": {
                "item": {
                    "type": "agentMessage",
                    "phase": "finalAnswer",
                    "text": "不得采信的拼接回复",
                }
            },
        }
        matching = {
            "method": "turn/completed",
            "payload": {
                "turn": {
                    "id": "turn-expected",
                    "status": "completed",
                }
            },
        }
        cases = (
            ("duplicate", [item, matching, matching]),
            (
                "wrong-then-matching",
                [
                    item,
                    {
                        "method": "turn/completed",
                        "payload": {
                            "turn": {
                                "id": "turn-other",
                                "status": "failed",
                            }
                        },
                    },
                    matching,
                ],
            ),
            ("item-after-completion", [matching, item]),
            (
                "event-after-completion",
                [matching, {"method": "future/event", "payload": 1}],
            ),
        )
        for case, records in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                settings, task, event_path, response_path = self._recovery_fixture(root)
                event_path.write_text(
                    "".join(
                        json.dumps(record, ensure_ascii=False) + "\n"
                        for record in records
                    ),
                    encoding="utf-8",
                )
                response_path.write_text("可信旧响应\n", encoding="utf-8")
                response_inode = response_path.stat().st_ino
                self._assert_recovery_rejected_without_state_change(
                    settings,
                    task,
                    response_path,
                    turn_id="turn-expected",
                    response_inode=response_inode,
                )

    def test_recovery_keeps_forward_compatible_unknown_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings, task, event_path, response_path = self._recovery_fixture(root)
            records = [
                {"method": "future/null", "payload": None, "extra": True},
                {"method": "future/scalar", "payload": 7},
                {"method": "future/array", "payload": [1, 2]},
                {
                    "method": "thread/tokenUsage/updated",
                    "payload": "invalid non-authoritative telemetry",
                },
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "type": "agentMessage",
                            "phase": "finalAnswer",
                            "text": "兼容恢复成功",
                            "futureField": {"nested": True},
                        },
                        "futurePayloadField": True,
                    },
                },
                {
                    "method": "turn/completed",
                    "payload": {
                        "turn": {
                            "id": "turn-expected",
                            "status": "completed",
                            "duration_ms": 9,
                            "futureTurnField": True,
                        }
                    },
                    "futureRecordField": True,
                },
            ]
            event_path.write_text(
                "\n"
                + "".join(
                    json.dumps(record, ensure_ascii=False) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )

            with prepare_store(settings, [task]) as store:
                self.assertEqual(store.task(task.task_id)["status"], "SUCCEEDED")
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "SUCCEEDED")
                self.assertEqual(attempt["turn_id"], "turn-expected")
                self.assertEqual(attempt["final_response"], "兼容恢复成功")
                self.assertEqual(attempt["duration_ms"], 9)
                self.assertIsNone(attempt["usage_json"])
            self.assertIn("兼容恢复成功", response_path.read_text(encoding="utf-8"))

    def test_recovery_rejects_invalid_utf8_without_state_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings, task, event_path, response_path = self._recovery_fixture(root)
            event_path.write_bytes(b"\xff\n")
            response_path.write_text("可信旧响应\n", encoding="utf-8")
            response_inode = response_path.stat().st_ino
            self._assert_recovery_rejected_without_state_change(
                settings,
                task,
                response_path,
                turn_id="turn-expected",
                response_inode=response_inode,
            )

    def test_recovery_finish_cas_conflict_does_not_create_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="恢复收尾 CAS 冲突测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("recover-conflict", 1, True, "A.md", "A()", "A.c")
            event_relative = "events/001-recover-conflict-a1.jsonl"
            response_relative = "responses/001-recover-conflict-a1.md"
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.set_meta("thread_id", "thread-shared")
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-recover-conflict-a1.txt",
                    event_file=event_relative,
                    response_file=response_relative,
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                )
                store.mark_turn_started(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                    turn_id="turn-recovery-conflict",
                )

            event_path = output_dir / event_relative
            event_path.parent.mkdir(parents=True)
            event_path.write_text(
                "".join(
                    json.dumps(item, ensure_ascii=False) + "\n"
                    for item in (
                        {
                            "method": "item/completed",
                            "payload": {
                                "item": {
                                    "root": {
                                        "type": "agentMessage",
                                        "phase": "finalAnswer",
                                        "text": "不应发布的恢复回复",
                                    }
                                }
                            },
                        },
                        {
                            "method": "turn/completed",
                            "payload": {
                                "turn": {
                                    "id": "turn-recovery-conflict",
                                    "status": "completed",
                                }
                            },
                        },
                    )
                ),
                encoding="utf-8",
            )
            response_path = output_dir / response_relative
            self.assertFalse(response_path.exists())
            real_finish_attempt = Store.finish_attempt

            def inject_conflict(
                store: Store,
                task_id: str,
                attempt_no: int,
                **kwargs,
            ) -> None:
                with store.conn:
                    store.conn.execute(
                        "UPDATE attempts SET status='INCOMPLETE' "
                        "WHERE task_id=? AND attempt_no=?",
                        (task_id, attempt_no),
                    )
                    store.conn.execute(
                        "UPDATE tasks SET status='INCOMPLETE' WHERE task_id=?",
                        (task_id,),
                    )
                real_finish_attempt(store, task_id, attempt_no, **kwargs)

            with mock.patch.object(
                Store,
                "finish_attempt",
                new=inject_conflict,
            ):
                with self.assertRaises(TurnCheckpointConflict):
                    prepare_store(settings, [task])

            self.assertFalse(response_path.exists())
            self.assertEqual(list(response_path.parent.iterdir()), [])
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task(task.task_id)["status"], "INCOMPLETE")
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "INCOMPLETE")
                self.assertIsNone(attempt["completed_at"])
                self.assertIsNone(attempt["final_response"])

    def test_recovery_rejects_event_and_response_leaf_links(self) -> None:
        for linked_field in ("event_file", "response_file"):
            with self.subTest(
                linked_field=linked_field
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="恢复工件链接保护测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=False,
                )
                task = TaskSpec(
                    "recover-link",
                    1,
                    True,
                    "A.md",
                    "A()",
                    "A.c",
                )
                event_relative = "events/001-recover-link-a1.jsonl"
                response_relative = "responses/001-recover-link-a1.md"
                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    store.set_meta("project_dir", str(project_dir))
                    store.set_meta("thread_id", "thread-shared")
                    store.start_attempt(
                        task,
                        attempt_no=1,
                        prompt=build_prompt(task, 1),
                        agents_snapshot=[],
                        prompt_file="prompts/001-recover-link-a1.txt",
                        event_file=event_relative,
                        response_file=response_relative,
                    )
                    store.mark_turn_start_requested(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                    )
                    store.mark_turn_started(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                        turn_id="turn-recover-link",
                    )

                records = (
                    {
                        "method": "item/completed",
                        "payload": {
                            "item": {
                                "root": {
                                    "type": "agentMessage",
                                    "phase": "finalAnswer",
                                    "text": "不得从链接恢复的回复",
                                }
                            }
                        },
                    },
                    {
                        "method": "turn/completed",
                        "payload": {
                            "turn": {
                                "id": "turn-recover-link",
                                "status": "completed",
                            }
                        },
                    },
                )
                completed_event = "".join(
                    json.dumps(item, ensure_ascii=False) + "\n" for item in records
                )
                protected = output_dir / "protected.txt"
                atomic_write_text(
                    output_dir,
                    "protected.txt",
                    completed_event,
                    field="protected_file",
                    expected_leaf="protected.txt",
                )
                event_path = output_dir / event_relative
                response_path = output_dir / response_relative
                if linked_field == "event_file":
                    event_path.parent.mkdir()
                    event_path.symlink_to(protected)
                    linked_path = event_path
                else:
                    atomic_write_text(
                        output_dir,
                        event_relative,
                        completed_event,
                        field="event_file",
                        expected_directory="events",
                    )
                    response_path.parent.mkdir()
                    response_path.symlink_to(protected)
                    linked_path = response_path

                with self.assertRaisesRegex(
                    ValueError,
                    f"{linked_field}.*符号链接",
                ):
                    prepare_store(settings, [task])

                self.assertTrue(linked_path.is_symlink())
                self.assertEqual(
                    protected.read_text(encoding="utf-8"),
                    completed_event,
                )
                with Store(output_dir / "run.sqlite3") as store:
                    self.assertEqual(store.task(task.task_id)["status"], "RUNNING")
                    attempt = store.latest_attempt(task.task_id)
                    self.assertEqual(attempt["status"], "RUNNING")
                    self.assertIsNone(attempt["completed_at"])
                    self.assertIsNone(attempt["final_response"])

    def test_recovery_event_read_rejects_descriptor_races_and_hard_links(
        self,
    ) -> None:
        for case in (
            "leaf-swap",
            "parent-swap",
            "hard-link",
            "hard-link-after-check",
        ):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="恢复事件描述符读取测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=False,
                )
                task = TaskSpec(
                    f"recover-{case}",
                    1,
                    True,
                    "A.md",
                    "A()",
                    "A.c",
                )
                event_relative = f"events/001-recover-{case}-a1.jsonl"
                response_relative = f"responses/001-recover-{case}-a1.md"
                turn_id = f"turn-recover-{case}"
                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    store.set_meta("project_dir", str(project_dir))
                    store.set_meta("thread_id", "thread-shared")
                    store.start_attempt(
                        task,
                        attempt_no=1,
                        prompt=build_prompt(task, 1),
                        agents_snapshot=[],
                        prompt_file=f"prompts/001-recover-{case}-a1.txt",
                        event_file=event_relative,
                        response_file=response_relative,
                    )
                    store.mark_turn_start_requested(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                    )
                    store.mark_turn_started(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                        turn_id=turn_id,
                    )

                completed_event = "".join(
                    json.dumps(item, ensure_ascii=False) + "\n"
                    for item in (
                        {
                            "method": "item/completed",
                            "payload": {
                                "item": {
                                    "root": {
                                        "type": "agentMessage",
                                        "phase": "finalAnswer",
                                        "text": "不得从替换后的路径恢复",
                                    }
                                }
                            },
                        },
                        {
                            "method": "turn/completed",
                            "payload": {
                                "turn": {
                                    "id": turn_id,
                                    "status": "completed",
                                }
                            },
                        },
                    )
                )
                protected = root / f"protected-{case}.jsonl"
                protected.write_text(completed_event, encoding="utf-8")
                event_path = output_dir / event_relative
                event_path.parent.mkdir()
                response_path = output_dir / response_relative

                swapped = False
                if case == "hard-link":
                    os.link(protected, event_path)
                    self.assertEqual(
                        os.stat(protected).st_ino,
                        os.stat(event_path).st_ino,
                    )
                    self.assertGreaterEqual(os.stat(event_path).st_nlink, 2)
                    descriptor_patch = contextlib.nullcontext()
                    expected_error = "event_file.*硬链接"
                else:
                    event_path.write_text(
                        completed_event if case == "hard-link-after-check" else "{}\n",
                        encoding="utf-8",
                    )
                    real_open_descriptor = open_descriptor

                    def swap_at_descriptor_open(
                        path,
                        flags,
                        mode=0o777,
                        *,
                        dir_fd=None,
                    ):
                        nonlocal swapped
                        component = Path(path).name
                        target_component = (
                            "events" if case == "parent-swap" else event_path.name
                        )
                        is_target = (
                            dir_fd is not None
                            and component == target_component
                            and not swapped
                        )
                        if is_target and case != "hard-link-after-check":
                            swapped = True
                            if case == "leaf-swap":
                                event_path.unlink()
                                event_path.symlink_to(protected)
                            else:
                                original_events = output_dir / "original-events"
                                event_path.parent.rename(original_events)
                                redirected_events = root / "redirected-events"
                                redirected_events.mkdir()
                                (redirected_events / event_path.name).write_text(
                                    completed_event,
                                    encoding="utf-8",
                                )
                                event_path.parent.symlink_to(redirected_events)

                        descriptor = real_open_descriptor(
                            path,
                            flags,
                            mode,
                            dir_fd=dir_fd,
                        )

                        if is_target and case == "hard-link-after-check":
                            swapped = True
                            first_alias = root / "event-alias-1.jsonl"
                            second_alias = root / "event-alias-2.jsonl"
                            os.link(event_path, first_alias)
                            os.link(event_path, second_alias)
                            event_path.unlink()
                            event_path.write_text("{}\n", encoding="utf-8")
                            self.assertEqual(
                                os.stat(first_alias).st_ino,
                                os.stat(second_alias).st_ino,
                            )
                            self.assertNotEqual(
                                os.stat(event_path).st_ino,
                                os.stat(first_alias).st_ino,
                            )
                            self.assertEqual(os.stat(first_alias).st_nlink, 2)
                            self.assertEqual(os.stat(event_path).st_nlink, 1)
                        return descriptor

                    descriptor_patch = mock.patch(
                        "codex_batch.open_descriptor",
                        new=swap_at_descriptor_open,
                    )
                    expected_error = (
                        "event_file.*硬链接"
                        if case == "hard-link-after-check"
                        else "event_file.*安全"
                    )

                with descriptor_patch, self.assertRaisesRegex(
                    ValueError,
                    expected_error,
                ):
                    prepare_store(settings, [task])

                self.assertEqual(protected.read_text(encoding="utf-8"), completed_event)
                if case != "hard-link":
                    self.assertTrue(swapped)
                if case == "leaf-swap":
                    self.assertTrue(event_path.is_symlink())
                elif case == "parent-swap":
                    self.assertTrue(event_path.parent.is_symlink())
                self.assertFalse(response_path.exists())
                with Store(output_dir / "run.sqlite3") as store:
                    self.assertEqual(store.task(task.task_id)["status"], "RUNNING")
                    attempt = store.latest_attempt(task.task_id)
                    self.assertEqual(attempt["status"], "RUNNING")
                    self.assertEqual(attempt["thread_id"], "thread-shared")
                    self.assertEqual(attempt["turn_id"], turn_id)
                    self.assertIsNone(attempt["completed_at"])
                    self.assertIsNone(attempt["final_response"])
                    self.assertIsNone(attempt["error_json"])

    def test_descriptor_event_read_distinguishes_missing_artifact_from_root(
        self,
    ) -> None:
        for existing_parent in (False, True):
            with self.subTest(
                existing_parent=existing_parent
            ), tempfile.TemporaryDirectory() as directory:
                output_dir = Path(directory) / "state"
                output_dir.mkdir()
                if existing_parent:
                    (output_dir / "events").mkdir()

                handle = open_output_text_for_read(
                    output_dir,
                    "events/missing.jsonl",
                    directory="events",
                    field="event_file",
                )

                self.assertIsNone(handle)
                self.assertFalse((output_dir / "events/missing.jsonl").exists())

        with tempfile.TemporaryDirectory() as directory:
            missing_root = Path(directory) / "missing-state"
            with self.assertRaises(FileNotFoundError):
                open_output_text_for_read(
                    missing_root,
                    "events/missing.jsonl",
                    directory="events",
                    field="event_file",
                )

    def test_descriptor_event_read_closes_all_owned_descriptors(self) -> None:
        for case in ("success", "missing", "hard-link", "fdopen-failure"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                output_dir = root / "state"
                output_dir.mkdir()
                event_relative = "events/recovery.jsonl"
                event_path = output_dir / event_relative
                if case != "missing":
                    event_path.parent.mkdir()
                    event_path.write_text("{}\n", encoding="utf-8")
                if case == "hard-link":
                    os.link(event_path, root / "event-alias.jsonl")

                real_open_descriptor = open_descriptor
                opened_descriptors: list[int] = []

                def tracking_open(path, flags, mode=0o777, *, dir_fd=None):
                    descriptor = real_open_descriptor(
                        path,
                        flags,
                        mode,
                        dir_fd=dir_fd,
                    )
                    opened_descriptors.append(descriptor)
                    return descriptor

                fdopen_patch = (
                    mock.patch(
                        "codex_batch.os.fdopen",
                        side_effect=OSError("injected fdopen failure"),
                    )
                    if case == "fdopen-failure"
                    else contextlib.nullcontext()
                )
                with mock.patch(
                    "codex_batch.open_descriptor",
                    new=tracking_open,
                ), fdopen_patch:
                    if case == "hard-link":
                        with self.assertRaisesRegex(ValueError, "硬链接"):
                            open_output_text_for_read(
                                output_dir,
                                event_relative,
                                directory="events",
                                field="event_file",
                            )
                    elif case == "fdopen-failure":
                        with self.assertRaisesRegex(OSError, "fdopen failure"):
                            open_output_text_for_read(
                                output_dir,
                                event_relative,
                                directory="events",
                                field="event_file",
                            )
                    elif case == "missing":
                        self.assertIsNone(
                            open_output_text_for_read(
                                output_dir,
                                event_relative,
                                directory="events",
                                field="event_file",
                            )
                        )
                    else:
                        handle = open_output_text_for_read(
                            output_dir,
                            event_relative,
                            directory="events",
                            field="event_file",
                        )
                        self.assertIsNotNone(handle)
                        with handle:
                            self.assertEqual(handle.read(), "{}\n")

                self.assertGreaterEqual(len(opened_descriptors), 1)
                for descriptor in set(opened_descriptors):
                    with self.assertRaises(OSError) as raised:
                        os.fstat(descriptor)
                    self.assertEqual(raised.exception.errno, errno.EBADF)

    def test_legacy_failed_recovers_only_with_matching_terminal_event(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="历史 FAILED 终态恢复测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("legacy-failed", 1, True, "A.md", "A()", "A.c")
            event_relative = "events/001-legacy-failed-a1.jsonl"
            response_relative = "responses/001-legacy-failed-a1.md"
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-legacy-failed-a1.txt",
                    event_file=event_relative,
                    response_file=response_relative,
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                )
                store.mark_turn_started(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                    turn_id="turn-legacy-failed",
                )
                with store.conn:
                    store.conn.execute(
                        "UPDATE attempts SET status='FAILED' WHERE task_id=?",
                        (task.task_id,),
                    )
                    store.conn.execute(
                        "UPDATE tasks SET status='FAILED' WHERE task_id=?",
                        (task.task_id,),
                    )

            event_path = output_dir / event_relative
            event_path.parent.mkdir(parents=True)
            event_path.write_text(
                json.dumps(
                    {
                        "method": "turn/completed",
                        "payload": {
                            "turn": {
                                "id": "turn-legacy-failed",
                                "status": "failed",
                                "error": "matching terminal failure",
                            }
                        },
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            with prepare_store(settings, [task]) as store:
                self.assertEqual(
                    store.task(task.task_id)["status"],
                    "FAILED_TERMINAL",
                )
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "FAILED_TERMINAL")
                self.assertEqual(attempt["turn_id"], "turn-legacy-failed")
                self.assertIn("matching terminal failure", attempt["error_json"])
                self.assertEqual(store.unresolved_tasks(), [])
                self.assertEqual(store.unresolved_attempts(), [])

    def test_recovery_does_not_rewrite_unknown_local_statuses(self) -> None:
        for location in ("task", "attempt", "hidden_attempt"):
            with self.subTest(
                location=location
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="未知本地状态恢复拒绝测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=False,
                )
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    store.set_meta("project_dir", str(project_dir))
                    store.start_attempt(
                        task,
                        attempt_no=1,
                        prompt=build_prompt(task, 1),
                        agents_snapshot=[],
                        prompt_file="prompts/task-a-a1.txt",
                        event_file="events/task-a-a1.jsonl",
                        response_file="responses/task-a-a1.md",
                    )
                    store.mark_turn_start_requested(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                    )
                    store.mark_turn_started(
                        task.task_id,
                        1,
                        thread_id="thread-shared",
                        turn_id="turn-1",
                    )
                    unknown_status = (
                        "FUTURE_TASK_STATE"
                        if location == "task"
                        else "FUTURE_ATTEMPT_STATE"
                    )
                    with store.conn:
                        if location == "task":
                            store.conn.execute(
                                "UPDATE tasks SET status=? WHERE task_id=?",
                                (unknown_status, task.task_id),
                            )
                        else:
                            store.conn.execute(
                                "UPDATE attempts SET status=? "
                                "WHERE task_id=? AND attempt_no=1",
                                (unknown_status, task.task_id),
                            )
                            if location == "hidden_attempt":
                                store.conn.execute(
                                    """
                                    INSERT INTO attempts(
                                        task_id, attempt_no, thread_id, turn_id,
                                        status, started_at, prompt,
                                        agents_snapshot_json, prompt_file,
                                        event_file, response_file
                                    ) VALUES(
                                        ?, 2, 'thread-shared', 'turn-2',
                                        'RUNNING', ?, ?, '[]', ?, ?, ?
                                    )
                                    """,
                                    (
                                        task.task_id,
                                        "injected-started-at",
                                        build_prompt(task, 2),
                                        "prompts/task-a-a2.txt",
                                        "events/task-a-a2.jsonl",
                                        "responses/task-a-a2.md",
                                    ),
                                )
                                store.conn.execute(
                                    "UPDATE tasks SET latest_attempt=2 "
                                    "WHERE task_id=?",
                                    (task.task_id,),
                                )

                    with mock.patch(
                        "codex_batch.recover_outcome_from_event_file",
                        return_value=None,
                    ) as recover_outcome:
                        recover_completed_attempts(store, settings)

                    recover_outcome.assert_not_called()
                    expected_task_status = (
                        unknown_status if location == "task" else "RUNNING"
                    )
                    expected_attempt_status = (
                        unknown_status if location == "attempt" else "RUNNING"
                    )
                    self.assertEqual(
                        store.task(task.task_id)["status"],
                        expected_task_status,
                    )
                    self.assertEqual(
                        store.latest_attempt(task.task_id)["status"],
                        expected_attempt_status,
                    )
                    if location == "hidden_attempt":
                        hidden_status = store.conn.execute(
                            "SELECT status FROM attempts "
                            "WHERE task_id=? AND attempt_no=1",
                            (task.task_id,),
                        ).fetchone()[0]
                        self.assertEqual(hidden_status, unknown_status)
                    self.assertFalse(
                        (output_dir / "responses/task-a-a1.md").exists()
                    )

    def test_recovered_unrecognized_status_remains_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="恢复未知终态测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            task = TaskSpec("recover-unknown", 1, True, "A.md", "A()", "A.c")
            event_relative = "events/001-recover-unknown-a1.jsonl"
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))
                store.start_attempt(
                    task,
                    attempt_no=1,
                    prompt=build_prompt(task, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/001-recover-unknown-a1.txt",
                    event_file=event_relative,
                    response_file="responses/001-recover-unknown-a1.md",
                )
                store.mark_turn_start_requested(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                )
                store.mark_turn_started(
                    task.task_id,
                    1,
                    thread_id="thread-shared",
                    turn_id="turn-unknown",
                )

            event_path = output_dir / event_relative
            event_path.parent.mkdir(parents=True)
            records = [
                {
                    "method": "item/completed",
                    "payload": {
                        "item": {
                            "root": {
                                "type": "agentMessage",
                                "phase": "finalAnswer",
                                "text": "存在回复但终态未知",
                            }
                        }
                    },
                },
                {
                    "method": "turn/completed",
                    "payload": {
                        "turn": {
                            "id": "turn-unknown",
                            "status": "future-unknown",
                        }
                    },
                },
            ]
            event_path.write_text(
                "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in records),
                encoding="utf-8",
            )

            with prepare_store(settings, [task]) as store:
                self.assertEqual(store.task(task.task_id)["status"], "INCOMPLETE")
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "INCOMPLETE")
                self.assertIn("future-unknown", attempt["error_json"])

    def test_settings_reject_output_that_overlaps_project(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            tasks_path = root / "tasks.csv"
            tasks_path.write_text(
                "id,enabled,document,function,source,prompt\n"
                "one,true,A.md,A(),A.c,\n",
                encoding="utf-8",
            )
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "project_dir": str(project_dir),
                        "tasks_file": str(tasks_path),
                        "output_dir": str(project_dir / ".batch"),
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "output_dir 必须与 project_dir"):
                load_settings(config_path)

    def test_private_state_and_atomic_write_do_not_follow_destination_link(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="安全写测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            ensure_secure_output_directory(settings)
            self.assertEqual(stat.S_IMODE(output_dir.stat().st_mode), 0o700)

            victim = root / "victim.txt"
            victim.write_text("SAFE", encoding="utf-8")
            response_path = output_dir / "responses" / "result.md"
            response_path.parent.mkdir()
            response_path.symlink_to(victim)

            atomic_write_text(
                output_dir,
                "responses/result.md",
                "EXPECTED",
                field="response_file",
                expected_directory="responses",
            )

            self.assertEqual(victim.read_text(encoding="utf-8"), "SAFE")
            self.assertFalse(response_path.is_symlink())
            self.assertEqual(response_path.read_text(encoding="utf-8"), "EXPECTED")
            self.assertEqual(stat.S_IMODE(response_path.stat().st_mode), 0o600)

    def test_staged_atomic_writer_rejects_parent_change_and_cleans_candidate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "state"
            output_dir.mkdir(mode=0o700)
            responses_dir = output_dir / "responses"
            responses_dir.mkdir(mode=0o700)
            original_responses_dir = output_dir / "responses-original"
            outside_dir = root / "outside"
            outside_dir.mkdir()
            outside_response = outside_dir / "result.md"
            outside_response.write_text("UNCHANGED\n", encoding="utf-8")
            original_inode = outside_response.stat().st_ino

            with staged_atomic_write_text(
                output_dir,
                "responses/result.md",
                "NEW RESPONSE\n",
                field="response_file",
                expected_directory="responses",
                fsync_parent=True,
            ) as publish:
                candidates = list(responses_dir.glob(".codex-batch-*.tmp"))
                self.assertEqual(len(candidates), 1)
                responses_dir.rename(original_responses_dir)
                responses_dir.symlink_to(outside_dir, target_is_directory=True)
                with self.assertRaisesRegex(ValueError, "父路径.*发生变化"):
                    publish()

            self.assertEqual(list(original_responses_dir.iterdir()), [])
            self.assertEqual(outside_response.stat().st_ino, original_inode)
            self.assertEqual(
                outside_response.read_text(encoding="utf-8"),
                "UNCHANGED\n",
            )
            self.assertEqual(
                sorted(path.name for path in outside_dir.iterdir()),
                [outside_response.name],
            )

    def test_staged_atomic_writer_closes_all_descriptors_and_cleans_temps(
        self,
    ) -> None:
        for case in ("success", "publish-failure"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                output_dir = Path(directory) / "state"
                output_dir.mkdir(mode=0o700)
                opened_descriptors: list[int] = []
                original_open_descriptor = open_descriptor

                def track_open_descriptor(
                    path,
                    flags,
                    mode=0o777,
                    *,
                    dir_fd=None,
                ):
                    descriptor = original_open_descriptor(
                        path,
                        flags,
                        mode,
                        dir_fd=dir_fd,
                    )
                    opened_descriptors.append(descriptor)
                    return descriptor

                publish_patch = (
                    mock.patch(
                        "codex_batch.replace_descriptor_path",
                        side_effect=OSError("injected publish failure"),
                    )
                    if case == "publish-failure"
                    else contextlib.nullcontext()
                )
                with mock.patch(
                    "codex_batch.open_descriptor",
                    side_effect=track_open_descriptor,
                ), publish_patch:
                    if case == "publish-failure":
                        with self.assertRaisesRegex(OSError, "publish failure"):
                            atomic_write_text(
                                output_dir,
                                "responses/nested/result.md",
                                "NEW RESPONSE\n",
                                field="response_file",
                                expected_directory="responses",
                                fsync_parent=True,
                            )
                    else:
                        atomic_write_text(
                            output_dir,
                            "responses/nested/result.md",
                            "NEW RESPONSE\n",
                            field="response_file",
                            expected_directory="responses",
                            fsync_parent=True,
                        )

                response_path = output_dir / "responses/nested/result.md"
                if case == "success":
                    self.assertEqual(
                        response_path.read_text(encoding="utf-8"),
                        "NEW RESPONSE\n",
                    )
                    self.assertEqual(
                        stat.S_IMODE(response_path.stat().st_mode),
                        0o600,
                    )
                else:
                    self.assertFalse(response_path.exists())
                self.assertEqual(
                    list((output_dir / "responses/nested").glob("*.tmp")),
                    [],
                )
                self.assertGreaterEqual(len(opened_descriptors), 4)
                for descriptor in set(opened_descriptors):
                    with self.assertRaises(OSError) as raised:
                        os.fstat(descriptor)
                    self.assertEqual(raised.exception.errno, errno.EBADF)

    def test_staged_atomic_writer_preserves_primary_error_and_closes_candidate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "state"
            output_dir.mkdir(mode=0o700)
            candidate_descriptors: list[int] = []
            original_open_descriptor = open_descriptor

            def track_candidate_descriptor(
                path,
                flags,
                mode=0o777,
                *,
                dir_fd=None,
            ):
                descriptor = original_open_descriptor(
                    path,
                    flags,
                    mode,
                    dir_fd=dir_fd,
                )
                if str(path).startswith(".codex-batch-"):
                    candidate_descriptors.append(descriptor)
                return descriptor

            with mock.patch(
                "codex_batch.open_descriptor",
                side_effect=track_candidate_descriptor,
            ), mock.patch(
                "codex_batch.replace_descriptor_path",
                side_effect=OSError("primary publish failure"),
            ), mock.patch(
                "codex_batch.unlink_descriptor_path",
                side_effect=OSError("secondary cleanup failure"),
            ):
                with self.assertRaises(OSError) as raised:
                    atomic_write_text(
                        output_dir,
                        "responses/result.md",
                        "NEW RESPONSE\n",
                        field="response_file",
                        expected_directory="responses",
                    )

            self.assertIn("primary publish failure", str(raised.exception))
            notes = getattr(raised.exception, "__notes__", [])
            if hasattr(raised.exception, "add_note"):
                self.assertTrue(
                    any("secondary cleanup failure" in note for note in notes)
                )
            self.assertEqual(len(candidate_descriptors), 1)
            with self.assertRaises(OSError) as descriptor_error:
                os.fstat(candidate_descriptors[0])
            self.assertEqual(descriptor_error.exception.errno, errno.EBADF)

    def test_staged_atomic_writer_closes_candidate_when_cleanup_is_interrupted(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "state"
            output_dir.mkdir(mode=0o700)
            candidate_descriptors: list[int] = []
            original_open_descriptor = open_descriptor

            def track_candidate_descriptor(
                path,
                flags,
                mode=0o777,
                *,
                dir_fd=None,
            ):
                descriptor = original_open_descriptor(
                    path,
                    flags,
                    mode,
                    dir_fd=dir_fd,
                )
                if str(path).startswith(".codex-batch-"):
                    candidate_descriptors.append(descriptor)
                return descriptor

            with mock.patch(
                "codex_batch.open_descriptor",
                side_effect=track_candidate_descriptor,
            ), mock.patch(
                "codex_batch.replace_descriptor_path",
                side_effect=OSError("primary publish failure"),
            ), mock.patch(
                "codex_batch.unlink_descriptor_path",
                side_effect=KeyboardInterrupt("secondary cleanup interrupt"),
            ):
                with self.assertRaises(OSError) as raised:
                    atomic_write_text(
                        output_dir,
                        "responses/result.md",
                        "NEW RESPONSE\n",
                        field="response_file",
                        expected_directory="responses",
                    )

            self.assertIn("primary publish failure", str(raised.exception))
            notes = getattr(raised.exception, "__notes__", [])
            if hasattr(raised.exception, "add_note"):
                self.assertTrue(
                    any("secondary cleanup interrupt" in note for note in notes)
                )
            self.assertEqual(len(candidate_descriptors), 1)
            with self.assertRaises(OSError) as descriptor_error:
                os.fstat(candidate_descriptors[0])
            self.assertEqual(descriptor_error.exception.errno, errno.EBADF)

    def test_private_event_writer_creates_anchored_private_file_and_closes_fds(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "state"
            output_dir.mkdir(mode=0o700)
            opened_descriptors: list[int] = []
            original_open_descriptor = open_descriptor

            def track_open_descriptor(
                path,
                flags,
                mode=0o777,
                *,
                dir_fd=None,
            ):
                descriptor = original_open_descriptor(
                    path,
                    flags,
                    mode,
                    dir_fd=dir_fd,
                )
                opened_descriptors.append(descriptor)
                return descriptor

            with mock.patch(
                "codex_batch.open_descriptor",
                side_effect=track_open_descriptor,
            ):
                with open_private_text_for_write(
                    output_dir,
                    "events/nested/event.jsonl",
                    directory="events",
                    field="event_file",
                    buffering=1,
                ) as handle:
                    handle.write("EVENT\n")

            event_path = output_dir / "events/nested/event.jsonl"
            self.assertEqual(event_path.read_text(encoding="utf-8"), "EVENT\n")
            self.assertEqual(stat.S_IMODE(event_path.stat().st_mode), 0o600)
            self.assertEqual(
                stat.S_IMODE((output_dir / "events").stat().st_mode),
                0o700,
            )
            self.assertEqual(
                stat.S_IMODE((output_dir / "events/nested").stat().st_mode),
                0o700,
            )
            self.assertGreaterEqual(len(opened_descriptors), 4)
            for descriptor in opened_descriptors:
                with self.subTest(descriptor=descriptor):
                    with self.assertRaises(OSError) as raised:
                        os.fstat(descriptor)
                    self.assertEqual(raised.exception.errno, errno.EBADF)

    def test_private_event_writer_checks_hardlinks_before_truncating(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "state"
            output_dir.mkdir(mode=0o700)
            events_dir = output_dir / "events"
            events_dir.mkdir(mode=0o700)
            event_path = events_dir / "event.jsonl"
            event_path.write_text("ORIGINAL\n", encoding="utf-8")
            event_path.chmod(0o600)
            alias_one = root / "alias-one.jsonl"
            alias_two = root / "alias-two.jsonl"
            leaf_descriptor: int | None = None
            original_open_descriptor = open_descriptor

            def add_links_after_leaf_open(
                path,
                flags,
                mode=0o777,
                *,
                dir_fd=None,
            ):
                nonlocal leaf_descriptor
                descriptor = original_open_descriptor(
                    path,
                    flags,
                    mode,
                    dir_fd=dir_fd,
                )
                if path == "event.jsonl" and flags & os.O_WRONLY:
                    self.assertEqual(flags & os.O_TRUNC, 0)
                    leaf_descriptor = descriptor
                    os.link(event_path, alias_one)
                    os.link(event_path, alias_two)
                return descriptor

            with mock.patch(
                "codex_batch.open_descriptor",
                side_effect=add_links_after_leaf_open,
            ):
                with self.assertRaisesRegex(ValueError, "event_file.*硬链接"):
                    open_private_text_for_write(
                        output_dir,
                        "events/event.jsonl",
                        directory="events",
                        field="event_file",
                    )

            self.assertIsNotNone(leaf_descriptor)
            with self.assertRaises(OSError) as raised:
                os.fstat(leaf_descriptor)
            self.assertEqual(raised.exception.errno, errno.EBADF)
            for path in (event_path, alias_one, alias_two):
                with self.subTest(path=path):
                    self.assertEqual(path.read_text(encoding="utf-8"), "ORIGINAL\n")
                    self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_private_event_writer_tightens_owned_parent_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "state"
            output_dir.mkdir(mode=0o700)
            events_dir = output_dir / "events"
            events_dir.mkdir(mode=0o755)
            events_dir.chmod(0o755)

            with open_private_text_for_write(
                output_dir,
                "events/event.jsonl",
                directory="events",
                field="event_file",
            ) as handle:
                handle.write("EVENT\n")

            self.assertEqual(stat.S_IMODE(events_dir.stat().st_mode), 0o700)
            event_path = events_dir / "event.jsonl"
            self.assertEqual(event_path.read_text(encoding="utf-8"), "EVENT\n")
            self.assertEqual(stat.S_IMODE(event_path.stat().st_mode), 0o600)

    def test_private_event_writer_closes_all_fds_when_fdopen_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "state"
            output_dir.mkdir(mode=0o700)
            opened_descriptors: list[int] = []
            original_open_descriptor = open_descriptor

            def track_open_descriptor(
                path,
                flags,
                mode=0o777,
                *,
                dir_fd=None,
            ):
                descriptor = original_open_descriptor(
                    path,
                    flags,
                    mode,
                    dir_fd=dir_fd,
                )
                opened_descriptors.append(descriptor)
                return descriptor

            with mock.patch(
                "codex_batch.open_descriptor",
                side_effect=track_open_descriptor,
            ), mock.patch(
                "codex_batch.os.fdopen",
                side_effect=OSError("injected fdopen failure"),
            ):
                with self.assertRaisesRegex(OSError, "injected fdopen failure"):
                    open_private_text_for_write(
                        output_dir,
                        "events/event.jsonl",
                        directory="events",
                        field="event_file",
                    )

            self.assertGreaterEqual(len(opened_descriptors), 3)
            for descriptor in opened_descriptors:
                with self.subTest(descriptor=descriptor):
                    with self.assertRaises(OSError) as raised:
                        os.fstat(descriptor)
                    self.assertEqual(raised.exception.errno, errno.EBADF)
            event_path = output_dir / "events/event.jsonl"
            self.assertEqual(event_path.read_bytes(), b"")
            self.assertEqual(stat.S_IMODE(event_path.stat().st_mode), 0o600)

    def test_private_event_writer_rejects_leaf_replacement_before_truncating(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "state"
            output_dir.mkdir(mode=0o700)
            events_dir = output_dir / "events"
            events_dir.mkdir(mode=0o700)
            event_path = events_dir / "event.jsonl"
            event_path.write_text("ORIGINAL\n", encoding="utf-8")
            event_path.chmod(0o600)
            moved_path = events_dir / "opened-original.jsonl"
            original_stat_descriptor_path = stat_descriptor_path
            replaced = False

            def replace_leaf_before_path_stat(path, *, dir_fd):
                nonlocal replaced
                if path == "event.jsonl" and not replaced:
                    replaced = True
                    event_path.rename(moved_path)
                    event_path.write_text("REPLACEMENT\n", encoding="utf-8")
                    event_path.chmod(0o600)
                return original_stat_descriptor_path(path, dir_fd=dir_fd)

            with mock.patch(
                "codex_batch.stat_descriptor_path",
                side_effect=replace_leaf_before_path_stat,
            ):
                with self.assertRaisesRegex(ValueError, "检查期间发生变化"):
                    open_private_text_for_write(
                        output_dir,
                        "events/event.jsonl",
                        directory="events",
                        field="event_file",
                    )

            self.assertTrue(replaced)
            self.assertEqual(moved_path.read_text(encoding="utf-8"), "ORIGINAL\n")
            self.assertEqual(event_path.read_text(encoding="utf-8"), "REPLACEMENT\n")
            self.assertEqual(stat.S_IMODE(moved_path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(event_path.stat().st_mode), 0o600)

    def test_private_event_writer_rechecks_links_immediately_before_truncating(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "state"
            output_dir.mkdir(mode=0o700)
            events_dir = output_dir / "events"
            events_dir.mkdir(mode=0o700)
            event_path = events_dir / "event.jsonl"
            event_path.write_text("ORIGINAL\n", encoding="utf-8")
            event_path.chmod(0o600)
            alias_path = root / "late-alias.jsonl"
            original_stat_descriptor_path = stat_descriptor_path
            linked = False

            def add_link_after_path_stat(path, *, dir_fd):
                nonlocal linked
                path_stat = original_stat_descriptor_path(path, dir_fd=dir_fd)
                if path == "event.jsonl" and not linked:
                    linked = True
                    os.link(event_path, alias_path)
                return path_stat

            with mock.patch(
                "codex_batch.stat_descriptor_path",
                side_effect=add_link_after_path_stat,
            ):
                with self.assertRaisesRegex(ValueError, "描述符.*发生变化"):
                    open_private_text_for_write(
                        output_dir,
                        "events/event.jsonl",
                        directory="events",
                        field="event_file",
                    )

            self.assertTrue(linked)
            self.assertEqual(event_path.read_text(encoding="utf-8"), "ORIGINAL\n")
            self.assertEqual(alias_path.read_text(encoding="utf-8"), "ORIGINAL\n")
            self.assertEqual(stat.S_IMODE(event_path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(alias_path.stat().st_mode), 0o600)

    def test_stored_artifact_path_cannot_leave_fixed_subdirectory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=root / "state",
                thread_name="路径测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            ensure_secure_output_directory(settings)

            with self.assertRaisesRegex(ValueError, "不是安全的输出相对路径"):
                safe_output_artifact_path(
                    settings,
                    "../outside.md",
                    directory="responses",
                    field="response_file",
                )
            with self.assertRaisesRegex(ValueError, "必须位于 responses"):
                safe_output_artifact_path(
                    settings,
                    "events/completed.jsonl",
                    directory="responses",
                    field="response_file",
                )
            outside_dir = root / "outside"
            outside_dir.mkdir()
            (settings.output_dir / "responses").symlink_to(
                outside_dir,
                target_is_directory=True,
            )
            with self.assertRaisesRegex(ValueError, "符号链接"):
                safe_output_artifact_path(
                    settings,
                    "responses/completed.md",
                    directory="responses",
                    field="response_file",
                )
            self.assertFalse((outside_dir / "completed.md").exists())
            (settings.output_dir / "responses").unlink()

            internal_dir = settings.output_dir / "events"
            internal_dir.mkdir()
            (settings.output_dir / "responses").symlink_to(
                internal_dir,
                target_is_directory=True,
            )
            with self.assertRaisesRegex(ValueError, "符号链接"):
                safe_output_artifact_path(
                    settings,
                    "responses/completed.md",
                    directory="responses",
                    field="response_file",
                )
            self.assertFalse((internal_dir / "completed.md").exists())
            (settings.output_dir / "responses").unlink()

            (settings.output_dir / "responses").symlink_to(
                settings.output_dir / "missing-directory",
                target_is_directory=True,
            )
            with self.assertRaisesRegex(ValueError, "符号链接"):
                safe_output_artifact_path(
                    settings,
                    "responses/completed.md",
                    directory="responses",
                    field="response_file",
                )
            (settings.output_dir / "responses").unlink()

            responses_dir = settings.output_dir / "responses"
            responses_dir.mkdir()
            nested_parent = responses_dir / "nested"
            nested_parent.symlink_to(
                root / "missing-nested-directory",
                target_is_directory=True,
            )
            with self.assertRaisesRegex(ValueError, "符号链接"):
                safe_output_artifact_path(
                    settings,
                    "responses/nested/completed.md",
                    directory="responses",
                    field="response_file",
                )
            nested_parent.unlink()
            nested_parent.write_text("not a directory", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "父路径必须是普通目录"):
                safe_output_artifact_path(
                    settings,
                    "responses/nested/completed.md",
                    directory="responses",
                    field="response_file",
                )

    def test_safe_output_artifact_path_rejects_leaf_links_and_non_files(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=root / "state",
                thread_name="工件叶子路径测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            ensure_secure_output_directory(settings)

            missing = settings.output_dir.resolve() / "responses/new.md"
            self.assertEqual(
                safe_output_artifact_path(
                    settings,
                    "responses/new.md",
                    directory="responses",
                    field="response_file",
                ),
                missing,
            )
            self.assertFalse(missing.parent.exists())

            nested = safe_output_artifact_path(
                settings,
                "responses/nested/result.md",
                directory="responses",
                field="response_file",
            )
            self.assertFalse(nested.parent.exists())
            atomic_write_text(
                settings.output_dir,
                "responses/nested/result.md",
                "NESTED\n",
                field="response_file",
                expected_directory="responses",
            )
            self.assertEqual(nested.read_text(encoding="utf-8"), "NESTED\n")

            responses_dir = settings.output_dir / "responses"
            responses_dir.mkdir(exist_ok=True)
            internal_target = settings.output_dir / "protected.txt"
            outside_target = root / "outside.txt"
            atomic_write_text(
                settings.output_dir,
                "protected.txt",
                "INTERNAL\n",
                field="protected_file",
                expected_leaf="protected.txt",
            )
            outside_target.write_text("OUTSIDE\n", encoding="utf-8")
            missing.symlink_to(internal_target)
            self.assertEqual(
                relative_output_path(settings, missing),
                "responses/new.md",
            )
            missing.unlink()
            cases = (
                ("internal", internal_target),
                ("outside", outside_target),
                ("dangling-internal", settings.output_dir / "missing.txt"),
                ("dangling-outside", root / "missing-outside.txt"),
            )
            for name, target in cases:
                with self.subTest(name=name):
                    artifact = responses_dir / f"{name}.md"
                    artifact.symlink_to(target)
                    with self.assertRaisesRegex(
                        ValueError,
                        "response_file.*符号链接",
                    ):
                        safe_output_artifact_path(
                            settings,
                            f"responses/{name}.md",
                            directory="responses",
                            field="response_file",
                        )
                    self.assertTrue(os.path.lexists(artifact))
                    self.assertTrue(artifact.is_symlink())
                    artifact.unlink()

            self.assertEqual(
                internal_target.read_text(encoding="utf-8"),
                "INTERNAL\n",
            )
            self.assertEqual(outside_target.read_text(encoding="utf-8"), "OUTSIDE\n")
            regular = responses_dir / "regular.md"
            atomic_write_text(
                settings.output_dir,
                "responses/regular.md",
                "REGULAR\n",
                field="response_file",
                expected_directory="responses",
            )
            self.assertEqual(
                safe_output_artifact_path(
                    settings,
                    "responses/regular.md",
                    directory="responses",
                    field="response_file",
                ),
                regular,
            )
            self.assertEqual(regular.read_text(encoding="utf-8"), "REGULAR\n")

            non_file = responses_dir / "directory.md"
            non_file.mkdir()
            with self.assertRaisesRegex(ValueError, "普通文件"):
                safe_output_artifact_path(
                    settings,
                    "responses/directory.md",
                    directory="responses",
                    field="response_file",
                )

    def test_batch_rejects_artifact_leaf_links_before_starting_turn(self) -> None:
        cases = (
            ("prompts", "txt", "prompt_file"),
            ("events", "jsonl", "event_file"),
            ("responses", "md", "response_file"),
        )
        for artifact_dir, suffix, field in cases:
            with self.subTest(
                artifact_dir=artifact_dir
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                (project_dir / "AGENTS.md").write_text(
                    "# Rules\n",
                    encoding="utf-8",
                )
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="工件链接启动保护测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=False,
                )
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                ensure_secure_output_directory(settings)
                protected = output_dir / "protected.txt"
                atomic_write_text(
                    output_dir,
                    "protected.txt",
                    "UNCHANGED\n",
                    field="protected_file",
                    expected_leaf="protected.txt",
                )
                base_name = attempt_artifact_basename(task.task_id, 1)
                artifact_path = (
                    output_dir / artifact_dir / f"{base_name}.{suffix}"
                )
                artifact_path.parent.mkdir()
                artifact_path.symlink_to(protected)

                world = FakeWorld()
                with contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(
                        ValueError,
                        f"{field}.*符号链接",
                    ):
                        execute_batch(settings, [task], sdk_api=fake_sdk(world))

                self.assertEqual(world.turn_request_count, 0)
                self.assertEqual(world.remote_accept_count, 0)
                self.assertEqual(world.codex_instances, 0)
                self.assertEqual(world.start_count, 0)
                self.assertEqual(world.resume_ids, [])
                self.assertTrue(artifact_path.is_symlink())
                self.assertEqual(
                    protected.read_text(encoding="utf-8"),
                    "UNCHANGED\n",
                )
                with Store(output_dir / "run.sqlite3") as store:
                    self.assertEqual(store.task(task.task_id)["status"], "PENDING")
                    self.assertIsNone(store.latest_attempt(task.task_id))

    def test_batch_rejects_event_hardlink_before_starting_turn(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="事件硬链接启动保护测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)

            protected = root / "protected.txt"
            protected.write_text("UNCHANGED\n", encoding="utf-8")
            protected.chmod(0o644)
            original_mode = stat.S_IMODE(protected.stat().st_mode)
            original_inode = protected.stat().st_ino
            event_path = (
                output_dir / self._attempt_artifact_relatives(task.task_id, 1)[1]
            )
            event_path.parent.mkdir(mode=0o700)
            os.link(protected, event_path)

            world = FakeWorld()
            with contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(
                    ValueError,
                    "event_file.*硬链接",
                ):
                    execute_batch(settings, [task], sdk_api=fake_sdk(world))

            self.assertEqual(world.codex_instances, 0)
            self.assertEqual(world.start_count, 0)
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            self.assertEqual(protected.read_text(encoding="utf-8"), "UNCHANGED\n")
            self.assertEqual(stat.S_IMODE(protected.stat().st_mode), original_mode)
            self.assertEqual(protected.stat().st_ino, original_inode)
            self.assertEqual(event_path.stat().st_ino, original_inode)
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task(task.task_id)["status"], "PENDING")
                self.assertIsNone(store.latest_attempt(task.task_id))

    def test_stream_event_parent_swap_stops_before_codex(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="事件父目录切换保护测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            events_dir = output_dir / "events"
            events_dir.mkdir(mode=0o700)
            outside_dir = root / "outside"
            outside_dir.mkdir()
            sentinel = outside_dir / "sentinel.txt"
            sentinel.write_text("UNCHANGED\n", encoding="utf-8")

            original_open_descriptor = open_descriptor
            swapped = False

            def swap_parent_before_open(
                path,
                flags,
                mode=0o777,
                *,
                dir_fd=None,
            ):
                nonlocal swapped
                if path == "events" and dir_fd is not None and not swapped:
                    swapped = True
                    events_dir.rename(output_dir / "events-original")
                    events_dir.symlink_to(outside_dir, target_is_directory=True)
                return original_open_descriptor(
                    path,
                    flags,
                    mode,
                    dir_fd=dir_fd,
                )

            world = FakeWorld()
            with mock.patch(
                "codex_batch.open_descriptor",
                side_effect=swap_parent_before_open,
            ), contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(settings, [task], sdk_api=fake_sdk(world))

            self.assertEqual(result, 1)
            self.assertTrue(swapped)
            self.assertEqual(world.codex_instances, 0)
            self.assertEqual(world.start_count, 0)
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            self.assertTrue(events_dir.is_symlink())
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "UNCHANGED\n")
            self.assertEqual(
                sorted(path.name for path in outside_dir.iterdir()),
                ["sentinel.txt"],
            )
            self.assertEqual(list((output_dir / "events-original").iterdir()), [])
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(
                    store.task(task.task_id)["status"],
                    "FAILED_BEFORE_REQUEST",
                )
                attempt = store.latest_attempt(task.task_id)
                self.assertEqual(attempt["status"], "FAILED_BEFORE_REQUEST")
                self.assertIsNone(attempt["thread_id"])
                self.assertIsNone(attempt["turn_id"])

    def test_response_parent_swap_cannot_publish_outside_output_dir(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="响应父目录切换保护测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            ensure_secure_output_directory(settings)
            responses_dir = output_dir / "responses"
            responses_dir.mkdir(mode=0o700)
            original_responses_dir = output_dir / "responses-original"
            outside_dir = root / "outside-responses"
            outside_dir.mkdir()
            outside_response = outside_dir / Path(
                self._attempt_artifact_relatives("task-a", 1)[2]
            ).name
            outside_response.write_text("UNCHANGED\n", encoding="utf-8")
            outside_response.chmod(0o644)
            original_inode = outside_response.stat().st_ino
            original_mode = stat.S_IMODE(outside_response.stat().st_mode)
            original_stream = FakeTurnHandle.stream
            swapped = False

            def swap_response_parent(handle):
                nonlocal swapped
                if not swapped:
                    swapped = True
                    responses_dir.rename(original_responses_dir)
                    responses_dir.symlink_to(
                        outside_dir,
                        target_is_directory=True,
                    )
                yield from original_stream(handle)

            world = FakeWorld()
            with mock.patch.object(
                FakeTurnHandle,
                "stream",
                new=swap_response_parent,
            ), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "response_file.*安全"):
                    execute_batch(
                        settings,
                        tasks,
                        limit=1,
                        sdk_api=fake_sdk(world),
                    )

            self.assertTrue(swapped)
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            self.assertEqual(outside_response.stat().st_ino, original_inode)
            self.assertEqual(
                stat.S_IMODE(outside_response.stat().st_mode),
                original_mode,
            )
            self.assertEqual(
                outside_response.read_text(encoding="utf-8"),
                "UNCHANGED\n",
            )
            self.assertEqual(
                sorted(path.name for path in outside_dir.iterdir()),
                [outside_response.name],
            )
            self.assertEqual(list(original_responses_dir.iterdir()), [])
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "RUNNING")
                attempt = store.latest_attempt("task-a")
                self.assertEqual(attempt["status"], "RUNNING")
                self.assertIsNone(attempt["completed_at"])
                self.assertIsNone(attempt["final_response"])
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

    def test_response_parent_swap_after_staging_rolls_back_and_cleans_candidate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="响应暂存后父目录切换保护测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            ensure_secure_output_directory(settings)
            responses_dir = output_dir / "responses"
            responses_dir.mkdir(mode=0o700)
            original_responses_dir = output_dir / "responses-original"
            outside_dir = root / "outside-responses"
            outside_dir.mkdir()
            outside_response = outside_dir / Path(
                self._attempt_artifact_relatives("task-a", 1)[2]
            ).name
            outside_response.write_text("UNCHANGED\n", encoding="utf-8")
            original_inode = outside_response.stat().st_ino
            real_finish_attempt = Store.finish_attempt
            swapped = False

            def swap_staged_response_parent(
                store: Store,
                task_id: str,
                attempt_no: int,
                **kwargs,
            ) -> None:
                nonlocal swapped
                if task_id == "task-a" and not swapped:
                    candidates = list(responses_dir.glob(".codex-batch-*.tmp"))
                    self.assertEqual(len(candidates), 1)
                    swapped = True
                    responses_dir.rename(original_responses_dir)
                    responses_dir.symlink_to(
                        outside_dir,
                        target_is_directory=True,
                    )
                real_finish_attempt(
                    store,
                    task_id,
                    attempt_no,
                    **kwargs,
                )

            world = FakeWorld()
            with mock.patch.object(
                Store,
                "finish_attempt",
                new=swap_staged_response_parent,
            ), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "父路径.*发生变化"):
                    execute_batch(
                        settings,
                        tasks,
                        limit=1,
                        sdk_api=fake_sdk(world),
                    )

            self.assertTrue(swapped)
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            self.assertEqual(outside_response.stat().st_ino, original_inode)
            self.assertEqual(
                outside_response.read_text(encoding="utf-8"),
                "UNCHANGED\n",
            )
            self.assertEqual(
                sorted(path.name for path in outside_dir.iterdir()),
                [outside_response.name],
            )
            self.assertEqual(list(original_responses_dir.iterdir()), [])
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "RUNNING")
                attempt = store.latest_attempt("task-a")
                self.assertEqual(attempt["status"], "RUNNING")
                self.assertEqual(attempt["thread_id"], "thread-shared")
                self.assertEqual(attempt["turn_id"], "turn-1")
                self.assertIsNone(attempt["completed_at"])
                self.assertIsNone(attempt["final_response"])
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

    def test_nonstream_event_parent_swap_cannot_write_outside_output_dir(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="非流式事件父目录切换保护测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=True,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            ensure_secure_output_directory(settings)
            events_dir = output_dir / "events"
            events_dir.mkdir(mode=0o700)
            original_events_dir = output_dir / "events-original"
            outside_dir = root / "outside-events"
            outside_dir.mkdir()
            outside_event = outside_dir / Path(
                self._attempt_artifact_relatives("task-a", 1)[1]
            ).name
            outside_event.write_text("UNCHANGED\n", encoding="utf-8")
            outside_event.chmod(0o644)
            original_inode = outside_event.stat().st_ino
            original_mode = stat.S_IMODE(outside_event.stat().st_mode)
            original_turn = FakeThread.turn
            swapped = False

            def turn_then_swap_events(thread, prompt, **kwargs):
                nonlocal swapped
                handle = original_turn(thread, prompt, **kwargs)
                if not swapped:
                    swapped = True
                    events_dir.rename(original_events_dir)
                    events_dir.symlink_to(
                        outside_dir,
                        target_is_directory=True,
                    )
                return handle

            world = FakeWorld()
            with mock.patch.object(
                FakeThread,
                "turn",
                new=turn_then_swap_events,
            ), contextlib.redirect_stdout(io.StringIO()):
                result = execute_batch(
                    settings,
                    tasks,
                    limit=1,
                    no_stream=True,
                    sdk_api=fake_sdk(world),
                )

            self.assertEqual(result, 1)
            self.assertTrue(swapped)
            self.assertEqual(world.turn_request_count, 1)
            self.assertEqual(world.remote_accept_count, 1)
            self.assertEqual(outside_event.stat().st_ino, original_inode)
            self.assertEqual(
                stat.S_IMODE(outside_event.stat().st_mode),
                original_mode,
            )
            self.assertEqual(
                outside_event.read_text(encoding="utf-8"),
                "UNCHANGED\n",
            )
            self.assertEqual(
                sorted(path.name for path in outside_dir.iterdir()),
                [outside_event.name],
            )
            self.assertEqual(list(original_events_dir.iterdir()), [])
            self.assertFalse(
                (
                    output_dir
                    / self._attempt_artifact_relatives("task-a", 1)[2]
                ).exists()
            )
            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.task("task-a")["status"], "INCOMPLETE")
                attempt = store.latest_attempt("task-a")
                self.assertEqual(attempt["status"], "INCOMPLETE")
                self.assertEqual(attempt["thread_id"], "thread-shared")
                self.assertEqual(attempt["turn_id"], "turn-1")
                self.assertIsNotNone(attempt["completed_at"])
                self.assertIsNone(attempt["final_response"])
                error = json.loads(attempt["error_json"])
                self.assertEqual(error["type"], "ValueError")
                self.assertIn("event_file", error["message"])
                self.assertEqual(store.task("task-b")["status"], "PENDING")
                self.assertIsNone(store.latest_attempt("task-b"))

    def test_sync_tasks_rolls_back_when_later_task_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            protected = TaskSpec("protected", 2, True, "A.md", "A()", "A.c")
            new_task = TaskSpec("new-task", 1, True, "N.md", "N()", "N.c")
            changed = TaskSpec("protected", 2, True, "B.md", "A()", "A.c")

            with Store(root / "run.sqlite3") as store:
                store.sync_tasks([protected])
                with store.conn:
                    store.conn.execute(
                        "UPDATE tasks SET status='SUCCEEDED' WHERE task_id=?",
                        (protected.task_id,),
                    )

                with self.assertRaisesRegex(ValueError, "已成功任务"):
                    store.sync_tasks([new_task, changed])

                self.assertFalse(store.conn.in_transaction)
                self.assertIsNone(store.task(new_task.task_id))
                protected_row = store.task(protected.task_id)
                self.assertEqual(protected_row["document"], protected.document)
                self.assertEqual(protected_row["status"], "SUCCEEDED")

    def test_set_meta_refuses_to_commit_callers_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            with Store(db_path) as store:
                store._upsert_meta_locked("caller_pending", "not-committed")
                self.assertTrue(store.conn.in_transaction)

                with self.assertRaisesRegex(RuntimeError, "已有 SQLite 事务"):
                    store.set_meta("thread_id", "thread-must-not-be-written")

                self.assertTrue(store.conn.in_transaction)
                self.assertEqual(
                    store.get_meta("caller_pending"),
                    "not-committed",
                )
                self.assertIsNone(store.get_meta("thread_id"))
                store.conn.rollback()
                self.assertIsNone(store.get_meta("caller_pending"))

    def test_delete_meta_refuses_to_commit_callers_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            with Store(db_path) as store:
                store.set_meta("target", "must-remain")
                store._upsert_meta_locked("caller_pending", "not-committed")
                self.assertTrue(store.conn.in_transaction)

                with self.assertRaisesRegex(RuntimeError, "已有 SQLite 事务"):
                    store.delete_meta("target")

                self.assertTrue(store.conn.in_transaction)
                self.assertEqual(store.get_meta("target"), "must-remain")
                self.assertEqual(
                    store.get_meta("caller_pending"),
                    "not-committed",
                )
                store.conn.rollback()
                self.assertEqual(store.get_meta("target"), "must-remain")
                self.assertIsNone(store.get_meta("caller_pending"))

                store.delete_meta("target")
                self.assertFalse(store.conn.in_transaction)
                self.assertIsNone(store.get_meta("target"))

            with Store(db_path) as store:
                self.assertIsNone(store.get_meta("target"))

    def test_state_transitions_refuse_to_commit_callers_transaction(self) -> None:
        for transition in (
            "start_attempt",
            "mark_turn_start_requested",
            "mark_turn_started",
            "finish_attempt",
        ):
            with self.subTest(
                transition=transition
            ), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                with Store(root / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    if transition != "start_attempt":
                        store.start_attempt(
                            task,
                            attempt_no=1,
                            prompt="transaction ownership",
                            agents_snapshot=[],
                            prompt_file="prompts/task-a.txt",
                            event_file="events/task-a.jsonl",
                            response_file="responses/task-a.md",
                        )
                    if transition in {"mark_turn_started", "finish_attempt"}:
                        store.mark_turn_start_requested(
                            task.task_id,
                            1,
                            thread_id="thread-shared",
                        )
                    if transition == "finish_attempt":
                        store.mark_turn_started(
                            task.task_id,
                            1,
                            thread_id="thread-shared",
                            turn_id="turn-1",
                        )

                    task_before = dict(store.task(task.task_id))
                    attempt_before_row = store.latest_attempt(task.task_id)
                    attempt_before = (
                        dict(attempt_before_row)
                        if attempt_before_row is not None
                        else None
                    )
                    store._upsert_meta_locked(
                        "caller_pending",
                        "not-committed",
                    )
                    self.assertTrue(store.conn.in_transaction)
                    published: list[bool] = []

                    with self.assertRaisesRegex(
                        RuntimeError,
                        "已有 SQLite 事务",
                    ):
                        if transition == "start_attempt":
                            store.start_attempt(
                                task,
                                attempt_no=1,
                                prompt="must not start",
                                agents_snapshot=[],
                                prompt_file="prompts/task-a.txt",
                                event_file="events/task-a.jsonl",
                                response_file="responses/task-a.md",
                            )
                        elif transition == "mark_turn_start_requested":
                            store.mark_turn_start_requested(
                                task.task_id,
                                1,
                                thread_id="thread-shared",
                            )
                        elif transition == "mark_turn_started":
                            store.mark_turn_started(
                                task.task_id,
                                1,
                                thread_id="thread-shared",
                                turn_id="turn-1",
                            )
                        else:
                            store.finish_attempt(
                                task.task_id,
                                1,
                                expected_task_status="RUNNING",
                                expected_attempt_status="RUNNING",
                                expected_turn_id="turn-1",
                                task_status="SUCCEEDED",
                                attempt_status="SUCCEEDED",
                                final_response="must not publish",
                                error=None,
                                usage=None,
                                duration_ms=1,
                                publish_response=lambda: published.append(True),
                            )

                    self.assertTrue(store.conn.in_transaction)
                    self.assertEqual(
                        store.get_meta("caller_pending"),
                        "not-committed",
                    )
                    self.assertEqual(dict(store.task(task.task_id)), task_before)
                    attempt_after_row = store.latest_attempt(task.task_id)
                    attempt_after = (
                        dict(attempt_after_row)
                        if attempt_after_row is not None
                        else None
                    )
                    self.assertEqual(attempt_after, attempt_before)
                    self.assertEqual(published, [])

                    store.conn.rollback()
                    self.assertIsNone(store.get_meta("caller_pending"))
                    self.assertEqual(dict(store.task(task.task_id)), task_before)

    def test_schema_bootstrap_meta_rolls_back_together(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            real_upsert = Store._upsert_meta_locked
            bootstrap_writes = 0

            def fail_second_bootstrap_write(
                store: Store,
                key: str,
                value: str,
            ) -> None:
                nonlocal bootstrap_writes
                real_upsert(store, key, value)
                if key in {"schema_version", "created_at"}:
                    bootstrap_writes += 1
                    if bootstrap_writes == 2:
                        raise RuntimeError("injected second bootstrap failure")

            with mock.patch.object(
                Store,
                "_upsert_meta_locked",
                new=fail_second_bootstrap_write,
            ):
                with self.assertRaisesRegex(RuntimeError, "bootstrap failure"):
                    Store(db_path)

            self.assertEqual(bootstrap_writes, 2)
            with contextlib.closing(sqlite3.connect(db_path)) as probe:
                meta_exists = probe.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='meta'"
                ).fetchone()
                bootstrap_meta = (
                    dict(
                        probe.execute(
                            "SELECT key, value FROM meta "
                            "WHERE key IN ('schema_version', 'created_at')"
                        )
                    )
                    if meta_exists
                    else {}
                )
            self.assertEqual(bootstrap_meta, {})

            with Store(db_path) as store:
                self.assertEqual(
                    store.get_meta("schema_version"),
                    str(SCHEMA_VERSION),
                )
                created_at = store.get_meta("created_at")
                self.assertIsNotNone(created_at)
                self.assertFalse(store.conn.in_transaction)

            with Store(db_path) as store:
                self.assertEqual(store.get_meta("created_at"), created_at)

    def test_store_rejects_existing_database_hardlink_before_connect(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="SQLite 硬链接拒绝测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            db_path = output_dir / "run.sqlite3"
            with Store(db_path) as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir))

            db_path.chmod(0o640)
            alias_path = root / "database-alias.sqlite3"
            os.link(db_path, alias_path)
            before_stat = db_path.stat()
            before_bytes = db_path.read_bytes()
            before_mode = stat.S_IMODE(before_stat.st_mode)
            before_mtime_ns = before_stat.st_mtime_ns
            self.assertEqual(before_stat.st_nlink, 2)
            self.assertEqual(alias_path.stat().st_ino, before_stat.st_ino)
            for suffix in ("-journal", "-wal", "-shm"):
                self.assertFalse(Path(f"{db_path}{suffix}").exists())

            world = FakeWorld()
            with mock.patch(
                "codex_batch.sqlite3.connect",
                wraps=sqlite3.connect,
            ) as connect:
                with self.assertRaisesRegex(ValueError, "SQLite.*硬链接"):
                    execute_batch(
                        settings,
                        [task],
                        sdk_api=fake_sdk(world),
                    )

            connect.assert_not_called()
            self.assertEqual(world.codex_instances, 0)
            self.assertEqual(world.start_count, 0)
            self.assertEqual(world.turn_request_count, 0)
            self.assertEqual(world.remote_accept_count, 0)
            after_stat = db_path.stat()
            self.assertEqual(db_path.read_bytes(), before_bytes)
            self.assertEqual(stat.S_IMODE(after_stat.st_mode), before_mode)
            self.assertEqual(after_stat.st_mtime_ns, before_mtime_ns)
            self.assertEqual(after_stat.st_ino, before_stat.st_ino)
            self.assertEqual(alias_path.stat().st_ino, before_stat.st_ino)
            self.assertEqual(after_stat.st_nlink, 2)
            for suffix in ("-journal", "-wal", "-shm"):
                self.assertFalse(Path(f"{db_path}{suffix}").exists())

    def test_store_rejects_hardlink_added_during_connect_before_writes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            (project_dir / "AGENTS.md").write_text(
                "# Rules\n",
                encoding="utf-8",
            )
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="SQLite connect 竞态拒绝测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            db_path = output_dir / "run.sqlite3"
            alias_path = root / "database-alias.sqlite3"
            self.assertFalse(os.path.lexists(db_path))
            real_connect = sqlite3.connect
            real_open_descriptor = open_descriptor
            connections: list[sqlite3.Connection] = []
            guard_descriptors: list[int] = []
            executed_sql: list[str] = []
            injected_snapshot: dict[str, Any] = {}

            def tracking_open_descriptor(*args, **kwargs):
                descriptor = real_open_descriptor(*args, **kwargs)
                guard_descriptors.append(descriptor)
                return descriptor

            def connect_then_link(*args, **kwargs):
                connection = real_connect(*args, **kwargs)
                connections.append(connection)
                connection.set_trace_callback(executed_sql.append)
                os.link(db_path, alias_path)
                linked_stat = db_path.stat()
                injected_snapshot.update(
                    bytes=db_path.read_bytes(),
                    mode=stat.S_IMODE(linked_stat.st_mode),
                    mtime_ns=linked_stat.st_mtime_ns,
                    inode=linked_stat.st_ino,
                )
                self.assertEqual(linked_stat.st_nlink, 2)
                return connection

            world = FakeWorld()
            try:
                with mock.patch(
                    "codex_batch.open_descriptor",
                    side_effect=tracking_open_descriptor,
                ), mock.patch(
                    "codex_batch.sqlite3.connect",
                    side_effect=connect_then_link,
                ):
                    with self.assertRaisesRegex(ValueError, "SQLite.*硬链接"):
                        execute_batch(
                            settings,
                            [task],
                            sdk_api=fake_sdk(world),
                        )

                self.assertEqual(len(connections), 1)
                self.assertEqual(executed_sql, [])
                with self.assertRaises(sqlite3.ProgrammingError):
                    connections[0].execute("SELECT 1")
                self.assertEqual(len(guard_descriptors), 1)
                with self.assertRaises(OSError) as closed_guard:
                    os.fstat(guard_descriptors[0])
                self.assertEqual(closed_guard.exception.errno, errno.EBADF)
                self.assertEqual(world.codex_instances, 0)
                self.assertEqual(world.start_count, 0)
                self.assertEqual(world.turn_request_count, 0)
                self.assertEqual(world.remote_accept_count, 0)
                after_stat = db_path.stat()
                self.assertEqual(db_path.read_bytes(), injected_snapshot["bytes"])
                self.assertEqual(
                    stat.S_IMODE(after_stat.st_mode),
                    injected_snapshot["mode"],
                )
                self.assertEqual(
                    after_stat.st_mtime_ns,
                    injected_snapshot["mtime_ns"],
                )
                self.assertEqual(after_stat.st_ino, injected_snapshot["inode"])
                self.assertEqual(
                    alias_path.stat().st_ino,
                    injected_snapshot["inode"],
                )
                self.assertEqual(after_stat.st_nlink, 2)
                for suffix in ("-journal", "-wal", "-shm"):
                    self.assertFalse(Path(f"{db_path}{suffix}").exists())
            finally:
                for connection in connections:
                    connection.close()

    def test_store_rechecks_hardlinks_after_transaction_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db_path = root / "run.sqlite3"
            alias_path = root / "database-alias.sqlite3"
            real_connect = sqlite3.connect
            link_after_commit = False

            class LinkingConnection(sqlite3.Connection):
                def commit(self):
                    result = super().commit()
                    if link_after_commit and not alias_path.exists():
                        os.link(db_path, alias_path)
                    return result

            def connect_with_commit_hook(*args, **kwargs):
                kwargs["factory"] = LinkingConnection
                return real_connect(*args, **kwargs)

            with mock.patch(
                "codex_batch.sqlite3.connect",
                side_effect=connect_with_commit_hook,
            ):
                store = Store(db_path)
                link_after_commit = True
                try:
                    with self.assertRaisesRegex(ValueError, "SQLite.*硬链接"):
                        store.set_meta("committed-before-recheck", "yes")
                    self.assertEqual(db_path.stat().st_nlink, 2)
                    alias_path.unlink()
                    self.assertEqual(
                        store.get_meta("committed-before-recheck"),
                        "yes",
                    )
                finally:
                    link_after_commit = False
                    if alias_path.exists():
                        alias_path.unlink()
                    store.close()

    def test_store_close_rechecks_hardlinks_and_releases_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db_path = root / "run.sqlite3"
            alias_path = root / "database-alias.sqlite3"
            store = Store(db_path)
            connection = store.conn
            guard_descriptor = store._database_guard_fd
            self.assertIsNotNone(guard_descriptor)
            os.link(db_path, alias_path)

            with self.assertRaisesRegex(ValueError, "SQLite.*硬链接"):
                store.close()

            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")
            with self.assertRaises(OSError) as closed_guard:
                os.fstat(guard_descriptor)
            self.assertEqual(closed_guard.exception.errno, errno.EBADF)

    def test_store_rejects_unsupported_schema_version_before_schema_changes(
        self,
    ) -> None:
        versions = {
            "future": str(SCHEMA_VERSION + 1),
            "old": str(SCHEMA_VERSION - 1),
            "invalid": "not-an-integer",
            "blank": "",
        }
        for label, stored_version in versions.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                db_path = Path(directory) / "run.sqlite3"
                with contextlib.closing(sqlite3.connect(db_path)) as setup:
                    setup.execute(
                        "CREATE TABLE meta ("
                        "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                    )
                    setup.execute(
                        "INSERT INTO meta(key, value) VALUES(?, ?)",
                        ("schema_version", stored_version),
                    )
                    setup.commit()

                opened: Store | None = None
                try:
                    with self.assertRaisesRegex(ValueError, "schema_version"):
                        opened = Store(db_path)
                finally:
                    if opened is not None:
                        opened.close()

                with contextlib.closing(sqlite3.connect(db_path)) as probe:
                    preserved_version = probe.execute(
                        "SELECT value FROM meta WHERE key='schema_version'"
                    ).fetchone()[0]
                    created_tables = probe.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE type='table' "
                        "AND name IN ('tasks', 'attempts')"
                    ).fetchall()
                self.assertEqual(preserved_version, stored_version)
                self.assertEqual(created_tables, [])

    def test_store_rejects_unversioned_or_incompatible_meta_before_schema_changes(
        self,
    ) -> None:
        for case in ("malformed", "state-without-version"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                db_path = Path(directory) / "run.sqlite3"
                with contextlib.closing(sqlite3.connect(db_path)) as setup:
                    if case == "malformed":
                        setup.execute(
                            "CREATE TABLE meta (key TEXT PRIMARY KEY)"
                        )
                    else:
                        setup.execute(
                            "CREATE TABLE meta ("
                            "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                        )
                        setup.execute(
                            "INSERT INTO meta(key, value) VALUES(?, ?)",
                            ("created_at", "existing-state"),
                        )
                    setup.commit()

                opened: Store | None = None
                try:
                    with self.assertRaisesRegex(
                        ValueError,
                        "meta|schema_version",
                    ):
                        opened = Store(db_path)
                finally:
                    if opened is not None:
                        opened.close()

                with contextlib.closing(sqlite3.connect(db_path)) as probe:
                    created_tables = probe.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE type='table' "
                        "AND name IN ('tasks', 'attempts')"
                    ).fetchall()
                    if case == "state-without-version":
                        created_at = probe.execute(
                            "SELECT value FROM meta WHERE key='created_at'"
                        ).fetchone()[0]
                        self.assertEqual(created_at, "existing-state")
                self.assertEqual(created_tables, [])

    def test_store_accepts_current_schema_version_and_preserves_created_at(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            with contextlib.closing(sqlite3.connect(db_path)) as setup:
                setup.execute(
                    "CREATE TABLE meta ("
                    "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                setup.executemany(
                    "INSERT INTO meta(key, value) VALUES(?, ?)",
                    (
                        ("schema_version", str(SCHEMA_VERSION)),
                        ("created_at", "preserved-created-at"),
                    ),
                )
                setup.commit()

            with Store(db_path) as store:
                self.assertEqual(
                    store.get_meta("schema_version"),
                    str(SCHEMA_VERSION),
                )
                self.assertEqual(
                    store.get_meta("created_at"),
                    "preserved-created-at",
                )
                self.assertFalse(store.conn.in_transaction)
                tables = {
                    row[0]
                    for row in store.conn.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE type='table' "
                        "AND name IN ('tasks', 'attempts')"
                    )
                }
                self.assertEqual(tables, {"tasks", "attempts"})

    def test_schema_version_is_rechecked_under_write_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            with contextlib.closing(sqlite3.connect(db_path)) as setup:
                setup.execute(
                    "CREATE TABLE meta ("
                    "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                setup.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?)",
                    ("schema_version", str(SCHEMA_VERSION)),
                )
                setup.commit()

            real_transaction = Store._immediate_transaction
            injected = False

            @contextlib.contextmanager
            def inject_future_version(store: Store):
                nonlocal injected
                if not injected:
                    with contextlib.closing(sqlite3.connect(db_path)) as other:
                        other.execute(
                            "UPDATE meta SET value=? WHERE key='schema_version'",
                            (str(SCHEMA_VERSION + 1),),
                        )
                        other.commit()
                    injected = True
                with real_transaction(store):
                    yield

            opened: Store | None = None
            with mock.patch.object(
                Store,
                "_immediate_transaction",
                new=inject_future_version,
            ):
                try:
                    with self.assertRaisesRegex(ValueError, "schema_version"):
                        opened = Store(db_path)
                finally:
                    if opened is not None:
                        opened.close()

            self.assertTrue(injected)
            with contextlib.closing(sqlite3.connect(db_path)) as probe:
                preserved_version = probe.execute(
                    "SELECT value FROM meta WHERE key='schema_version'"
                ).fetchone()[0]
                created_tables = probe.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' "
                    "AND name IN ('tasks', 'attempts')"
                ).fetchall()
            self.assertEqual(preserved_version, str(SCHEMA_VERSION + 1))
            self.assertEqual(created_tables, [])

    def test_store_rejects_constraint_mismatched_partial_bootstrap(self) -> None:
        broken_tasks = Store._SCHEMA_DDL[1].replace(
            "latest_attempt INTEGER NOT NULL DEFAULT 0",
            "latest_attempt INTEGER",
        )
        broken_attempts = Store._SCHEMA_DDL[2].replace(
            ",\n            FOREIGN KEY (task_id) REFERENCES tasks(task_id)",
            "",
        )
        unique_tasks = Store._SCHEMA_DDL[1].replace(
            "updated_at TEXT NOT NULL\n        )",
            "updated_at TEXT NOT NULL,\n            UNIQUE(document)\n        )",
        )
        check_tasks = Store._SCHEMA_DDL[1].replace(
            "updated_at TEXT NOT NULL\n        )",
            "updated_at TEXT NOT NULL,\n"
            "            CHECK(latest_attempt >= 0)\n        )",
        )
        collated_tasks = Store._SCHEMA_DDL[1].replace(
            "document TEXT NOT NULL,",
            "document TEXT NOT NULL COLLATE NOCASE,",
        )
        strict_tasks = Store._SCHEMA_DDL[1].rstrip() + " STRICT"
        without_rowid_tasks = Store._SCHEMA_DDL[1].rstrip() + " WITHOUT ROWID"
        task_cases = {
            "task-constraints": broken_tasks,
            "extra-unique": unique_tasks,
            "extra-check": check_tasks,
            "extra-collation": collated_tasks,
            "strict-table": strict_tasks,
            "without-rowid": without_rowid_tasks,
        }
        for statement in task_cases.values():
            self.assertNotEqual(statement, Store._SCHEMA_DDL[1])
        self.assertNotEqual(broken_attempts, Store._SCHEMA_DDL[2])

        for case in (*task_cases, "attempt-foreign-key"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                db_path = Path(directory) / "run.sqlite3"
                with contextlib.closing(sqlite3.connect(db_path)) as setup:
                    setup.execute(Store._SCHEMA_DDL[0])
                    if case == "attempt-foreign-key":
                        setup.execute(Store._SCHEMA_DDL[1])
                        setup.execute(broken_attempts)
                    else:
                        setup.execute(task_cases[case])
                    setup.commit()

                opened: Store | None = None
                try:
                    with self.assertRaisesRegex(ValueError, "表结构.*不兼容"):
                        opened = Store(db_path)
                finally:
                    if opened is not None:
                        opened.close()

                with contextlib.closing(sqlite3.connect(db_path)) as probe:
                    bootstrap_meta = probe.execute(
                        "SELECT key, value FROM meta ORDER BY key"
                    ).fetchall()
                self.assertEqual(bootstrap_meta, [])

    def test_store_recovers_exact_empty_partial_bootstrap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            with contextlib.closing(sqlite3.connect(db_path)) as setup:
                for statement in Store._SCHEMA_DDL:
                    format_variant = statement
                    for original, replacement in (
                        (
                            "CREATE TABLE IF NOT EXISTS",
                            "create\n table\tif  not exists",
                        ),
                        (" TEXT", " text"),
                        (" INTEGER", " integer"),
                        (" NOT NULL", " not\n null"),
                        (" PRIMARY KEY", " primary\tkey"),
                        (" DEFAULT ", " default\t"),
                        (" FOREIGN KEY", " foreign\n key"),
                        (" REFERENCES ", " references\t"),
                    ):
                        format_variant = format_variant.replace(
                            original,
                            replacement,
                        )
                    self.assertNotEqual(format_variant, statement)
                    setup.execute(format_variant)
                setup.commit()

            with Store(db_path) as store:
                self.assertEqual(
                    store.get_meta("schema_version"),
                    str(SCHEMA_VERSION),
                )
                self.assertIsNotNone(store.get_meta("created_at"))
                self.assertFalse(store.conn.in_transaction)

    def test_store_init_closes_connection_when_schema_initialization_fails(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            real_connect = sqlite3.connect
            connections: list[sqlite3.Connection] = []

            def tracking_connect(*args, **kwargs):
                connection = real_connect(*args, **kwargs)
                connections.append(connection)
                return connection

            try:
                with mock.patch(
                    "codex_batch.sqlite3.connect",
                    side_effect=tracking_connect,
                ), mock.patch.object(
                    Store,
                    "_create_schema",
                    side_effect=RuntimeError("injected schema failure"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "schema failure"):
                        Store(db_path)

                self.assertEqual(len(connections), 1)
                with self.assertRaises(sqlite3.ProgrammingError):
                    connections[0].execute("SELECT 1")
            finally:
                for connection in connections:
                    connection.close()

    def test_store_retries_transient_lock_while_enabling_wal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            real_connect = sqlite3.connect
            with contextlib.closing(real_connect(db_path)) as setup:
                setup.execute("CREATE TABLE marker(value TEXT NOT NULL)")
                setup.execute("INSERT INTO marker(value) VALUES('held')")
                setup.commit()

            blocker: sqlite3.Connection | None = real_connect(db_path)
            blocker.execute("BEGIN")
            blocker.execute("SELECT value FROM marker").fetchone()
            lock_errors: list[tuple[str, sqlite3.OperationalError]] = []

            class ReleasingConnection(sqlite3.Connection):
                def execute(self, sql, parameters=()):
                    nonlocal blocker
                    try:
                        return super().execute(sql, parameters)
                    except sqlite3.OperationalError as exc:
                        if sql.strip().casefold().startswith(
                            "pragma journal_mode"
                        ):
                            lock_errors.append((sql, exc))
                            if blocker is not None:
                                blocker.rollback()
                                blocker.close()
                                blocker = None
                        raise

            def connect_without_busy_wait(*args, **kwargs):
                kwargs["timeout"] = 0
                kwargs["factory"] = ReleasingConnection
                return real_connect(*args, **kwargs)

            try:
                with mock.patch(
                    "codex_batch.sqlite3.connect",
                    side_effect=connect_without_busy_wait,
                ), mock.patch.object(Store, "_create_schema", return_value=None):
                    with Store(db_path) as store:
                        journal_mode = store.conn.execute(
                            "PRAGMA journal_mode"
                        ).fetchone()[0]
            finally:
                if blocker is not None:
                    blocker.close()

            self.assertEqual(len(lock_errors), 1)
            lock_sql, lock_error = lock_errors[0]
            self.assertEqual(
                " ".join(lock_sql.split()).casefold(),
                "pragma journal_mode = wal",
            )
            self.assertEqual(str(lock_error), "database is locked")
            error_code = getattr(lock_error, "sqlite_errorcode", None)
            if isinstance(error_code, int):
                self.assertEqual(error_code & 0xFF, 5)
            self.assertEqual(str(journal_mode).casefold(), "wal")

    def test_store_rejects_wal_change_that_does_not_return_wal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            real_connect = sqlite3.connect
            wal_attempts: list[str] = []

            class RefusingWalConnection(sqlite3.Connection):
                def execute(self, sql, parameters=()):
                    normalized = " ".join(sql.split()).casefold()
                    if normalized == "pragma journal_mode = wal":
                        wal_attempts.append(sql)
                        return SimpleNamespace(fetchone=lambda: ("delete",))
                    return super().execute(sql, parameters)

            def connect_refusing_wal(*args, **kwargs):
                kwargs["factory"] = RefusingWalConnection
                return real_connect(*args, **kwargs)

            with mock.patch(
                "codex_batch.sqlite3.connect",
                side_effect=connect_refusing_wal,
            ), mock.patch.object(
                Store,
                "_create_schema",
                return_value=None,
            ), mock.patch(
                "codex_batch.SQLITE_WAL_RETRY_TIMEOUT_SECONDS",
                0,
                create=True,
            ):
                with self.assertRaisesRegex(RuntimeError, "journal_mode|WAL"):
                    Store(db_path)

            self.assertEqual(len(wal_attempts), 1)

    def test_store_does_not_retry_non_lock_wal_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "run.sqlite3"
            real_connect = sqlite3.connect
            wal_attempts: list[str] = []

            class FailingWalConnection(sqlite3.Connection):
                def execute(self, sql, parameters=()):
                    normalized = " ".join(sql.split()).casefold()
                    if normalized == "pragma journal_mode = wal":
                        wal_attempts.append(sql)
                        failure = sqlite3.OperationalError(
                            "injected WAL I/O failure"
                        )
                        failure.sqlite_errorcode = 10
                        raise failure
                    return super().execute(sql, parameters)

            def connect_failing_wal(*args, **kwargs):
                kwargs["factory"] = FailingWalConnection
                return real_connect(*args, **kwargs)

            with mock.patch(
                "codex_batch.sqlite3.connect",
                side_effect=connect_failing_wal,
            ), mock.patch.object(
                Store,
                "_create_schema",
                return_value=None,
            ), mock.patch("codex_batch.time.sleep") as sleep:
                with self.assertRaisesRegex(
                    sqlite3.OperationalError,
                    "injected WAL I/O failure",
                ):
                    Store(db_path)

            self.assertEqual(len(wal_attempts), 1)
            sleep.assert_not_called()

    def test_store_recognizes_python310_sqlite_lock_messages(self) -> None:
        self.assertTrue(
            is_sqlite_lock_error(
                sqlite3.OperationalError("database is locked")
            )
        )
        self.assertTrue(
            is_sqlite_lock_error(
                sqlite3.OperationalError(
                    "database table is locked: sqlite_master"
                )
            )
        )
        self.assertFalse(
            is_sqlite_lock_error(
                sqlite3.OperationalError("injected WAL I/O failure")
            )
        )

    def test_prepare_store_refuses_unbound_database_with_project_state(self) -> None:
        for state_kind in ("task", "meta"):
            with self.subTest(state_kind=state_kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "config.json",
                    project_dir=project_dir,
                    tasks_file=root / "tasks.csv",
                    output_dir=output_dir,
                    thread_name="缺失项目身份的旧状态库测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=False,
                )
                old_task = TaskSpec(
                    "old-task",
                    1,
                    True,
                    "Old.md",
                    "Old()",
                    "Old.c",
                )
                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    if state_kind == "task":
                        store.sync_tasks([old_task])
                    else:
                        store.set_meta("thread_id", "thread-from-old-project")
                    self.assertIsNone(store.get_meta("project_dir"))

                created_stores: list[Store] = []

                def tracking_store(db_path: Path) -> Store:
                    store = Store(db_path)
                    created_stores.append(store)
                    return store

                prepared: Store | None = None
                with mock.patch(
                    "codex_batch.Store",
                    side_effect=tracking_store,
                ):
                    try:
                        with self.assertRaisesRegex(ValueError, "缺少 project_dir"):
                            prepared = prepare_store(settings, [old_task])
                    finally:
                        if prepared is not None:
                            prepared.close()

                self.assertEqual(len(created_stores), 1)
                with self.assertRaises(sqlite3.ProgrammingError):
                    created_stores[0].conn.execute("SELECT 1")

                with Store(output_dir / "run.sqlite3") as store:
                    self.assertIsNone(store.get_meta("project_dir"))
                    if state_kind == "task":
                        self.assertIsNotNone(store.task(old_task.task_id))
                        self.assertIsNone(store.get_meta("thread_id"))
                    else:
                        self.assertIsNone(store.task(old_task.task_id))
                        self.assertEqual(
                            store.get_meta("thread_id"),
                            "thread-from-old-project",
                        )

    def test_prepare_store_rolls_back_sync_and_closes_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "config.json",
                project_dir=project_dir,
                tasks_file=root / "tasks.csv",
                output_dir=output_dir,
                thread_name="准备阶段同步回滚测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            tasks = [
                TaskSpec("task-a", 1, True, "A.md", "A()", "A.c"),
                TaskSpec("task-b", 2, True, "B.md", "B()", "B.c"),
            ]
            ensure_secure_output_directory(settings)

            created_stores: list[Store] = []
            real_task = Store.task

            def tracking_store(db_path: Path) -> Store:
                store = Store(db_path)
                created_stores.append(store)
                return store

            def fail_second_lookup(store: Store, task_id: str):
                if task_id == "task-b":
                    raise RuntimeError("injected task sync failure")
                return real_task(store, task_id)

            with mock.patch(
                "codex_batch.Store",
                side_effect=tracking_store,
            ), mock.patch.object(
                Store,
                "task",
                new=fail_second_lookup,
            ):
                with self.assertRaisesRegex(RuntimeError, "task sync failure"):
                    prepare_store(settings, tasks)

            self.assertEqual(len(created_stores), 1)
            with self.assertRaises(sqlite3.ProgrammingError):
                created_stores[0].conn.execute("SELECT 1")

            db_path = output_dir / "run.sqlite3"
            with contextlib.closing(sqlite3.connect(db_path, timeout=0)) as probe:
                probe.execute("BEGIN IMMEDIATE")
                probe.rollback()
            with Store(db_path) as store:
                self.assertEqual(
                    store.get_meta("project_dir"),
                    str(project_dir.resolve()),
                )
                self.assertIsNone(store.get_meta("config_path"))
                self.assertIsNone(store.get_meta("tasks_file"))
                self.assertEqual(
                    store.conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0],
                    0,
                )

    def test_prepare_store_defers_invocation_meta_until_ready(self) -> None:
        for failure_stage in ("recovery", "report"):
            with self.subTest(stage=failure_stage), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project_dir = root / "project"
                project_dir.mkdir()
                output_dir = root / "state"
                settings = Settings(
                    config_path=root / "new-config.json",
                    project_dir=project_dir,
                    tasks_file=root / "new-tasks.csv",
                    output_dir=output_dir,
                    thread_name="调用来源元数据延迟提交测试",
                    model=None,
                    effort=None,
                    stream_events=True,
                    continue_on_error=False,
                )
                task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
                ensure_secure_output_directory(settings)
                with Store(output_dir / "run.sqlite3") as store:
                    store.sync_tasks([task])
                    store.set_meta("project_dir", str(project_dir.resolve()))
                    store.set_meta("config_path", "old-config.json")
                    store.set_meta("tasks_file", "old-tasks.csv")

                target = (
                    "codex_batch.recover_completed_attempts"
                    if failure_stage == "recovery"
                    else "codex_batch.render_chat_report"
                )
                created_stores: list[Store] = []

                def tracking_store(db_path: Path) -> Store:
                    store = Store(db_path)
                    created_stores.append(store)
                    return store

                with mock.patch(
                    "codex_batch.Store",
                    side_effect=tracking_store,
                ), mock.patch(
                    target,
                    side_effect=RuntimeError(f"injected {failure_stage} failure"),
                ):
                    with self.assertRaisesRegex(RuntimeError, f"{failure_stage} failure"):
                        prepare_store(settings, [task])

                self.assertEqual(len(created_stores), 1)
                with self.assertRaises(sqlite3.ProgrammingError):
                    created_stores[0].conn.execute("SELECT 1")

                with Store(output_dir / "run.sqlite3") as store:
                    self.assertEqual(store.get_meta("config_path"), "old-config.json")
                    self.assertEqual(store.get_meta("tasks_file"), "old-tasks.csv")

                with prepare_store(settings, [task]) as store:
                    self.assertEqual(
                        store.get_meta("config_path"),
                        str(settings.config_path),
                    )
                    self.assertEqual(
                        store.get_meta("tasks_file"),
                        str(settings.tasks_file),
                    )
                    self.assertFalse(store.conn.in_transaction)

    def test_prepare_store_updates_invocation_meta_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_dir = root / "project"
            project_dir.mkdir()
            output_dir = root / "state"
            settings = Settings(
                config_path=root / "new-config.json",
                project_dir=project_dir,
                tasks_file=root / "new-tasks.csv",
                output_dir=output_dir,
                thread_name="调用来源元数据原子提交测试",
                model=None,
                effort=None,
                stream_events=True,
                continue_on_error=False,
            )
            task = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            ensure_secure_output_directory(settings)
            with Store(output_dir / "run.sqlite3") as store:
                store.sync_tasks([task])
                store.set_meta("project_dir", str(project_dir.resolve()))
                store.set_meta("config_path", "old-config.json")
                store.set_meta("tasks_file", "old-tasks.csv")

            real_upsert = Store._upsert_meta_locked

            def fail_second_invocation_meta(
                store: Store,
                key: str,
                value: str,
            ) -> None:
                real_upsert(store, key, value)
                if key == "tasks_file" and value == str(settings.tasks_file):
                    raise RuntimeError("injected invocation meta failure")

            created_stores: list[Store] = []

            def tracking_store(db_path: Path) -> Store:
                store = Store(db_path)
                created_stores.append(store)
                return store

            with mock.patch(
                "codex_batch.Store",
                side_effect=tracking_store,
            ), mock.patch.object(
                Store,
                "_upsert_meta_locked",
                new=fail_second_invocation_meta,
            ):
                with self.assertRaisesRegex(RuntimeError, "invocation meta failure"):
                    prepare_store(settings, [task])

            self.assertEqual(len(created_stores), 1)
            with self.assertRaises(sqlite3.ProgrammingError):
                created_stores[0].conn.execute("SELECT 1")

            with Store(output_dir / "run.sqlite3") as store:
                self.assertEqual(store.get_meta("config_path"), "old-config.json")
                self.assertEqual(store.get_meta("tasks_file"), "old-tasks.csv")
                self.assertFalse(store.conn.in_transaction)

    def test_unresolved_task_cannot_change_or_be_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store_path = root / "run.sqlite3"
            original = TaskSpec("task-a", 1, True, "A.md", "A()", "A.c")
            with Store(store_path) as store:
                store.sync_tasks([original])
                store.start_attempt(
                    original,
                    attempt_no=1,
                    prompt=build_prompt(original, 1),
                    agents_snapshot=[],
                    prompt_file="prompts/a.txt",
                    event_file="events/a.jsonl",
                    response_file="responses/a.md",
                )
                store.mark_turn_start_requested(
                    original.task_id,
                    1,
                    thread_id="thread-a",
                )
                store.mark_turn_started(
                    original.task_id,
                    1,
                    thread_id="thread-a",
                    turn_id="turn-a",
                )
                changed = TaskSpec("task-a", 1, True, "B.md", "A()", "A.c")
                with self.assertRaisesRegex(ValueError, "未完成任务"):
                    store.sync_tasks([changed])
                disabled = TaskSpec("task-a", 1, False, "A.md", "A()", "A.c")
                with self.assertRaisesRegex(ValueError, "enabled"):
                    store.sync_tasks([disabled])


if __name__ == "__main__":
    unittest.main()
