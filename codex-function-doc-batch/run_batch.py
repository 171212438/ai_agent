from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from codex_batch import (
  dry_run_output,
  ensure_secure_output_directory,
  execute_batch,
  is_sqlite_lock_error,
  load_settings,
  load_tasks,
  prepare_store,
  render_chat_report,
  RunLock,
  status_lines,
)


def build_parser() -> argparse.ArgumentParser:
  parser = argparse.ArgumentParser(description="使用 Python Codex SDK 串行处理函数文档；一个函数一个 Turn。")
  parser.add_argument("--config", type=Path, default=Path("config.json"), help="配置文件路径，默认 config.json。")
  mode_group = parser.add_mutually_exclusive_group()
  mode_group.add_argument("--dry-run", action="store_true", help="只校验并显示逐函数 Prompt，不启动 Codex。")
  mode_group.add_argument("--status", action="store_true", help="只显示当前检查点状态。")
  parser.add_argument("--limit", type=int, help="本次最多处理多少个待处理函数；建议首次使用 2。")
  parser.add_argument("--no-stream", action="store_true", help="不显示实时事件，直接等待 TurnResult。")
  parser.add_argument("--new-thread", action="store_true", help="让尚未完成的任务改用一个新的 Thread。")
  parser.add_argument(
    "--rerun", action="append", default=[], metavar="TASK_ID", help="重新执行 SUCCEEDED、FAILED_BEFORE_REQUEST 或 FAILED_TERMINAL 任务；可重复使用。"
  )
  parser.add_argument(
    "--retry-incomplete", action="append", default=[], metavar="TASK_ID", help="仅重试本地可证明尚未发出 Turn 请求的 TURN_NOT_REQUESTED 任务；可重复使用。"
  )
  parser.add_argument("--force-unlock", action="store_true", help="仅迁移经 PID 确认已经退出的旧版运行锁；不会覆盖活跃或无法验证的锁。")
  return parser

def validate_mode_options(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
  if args.status:
    mode = "--status"
    forbidden = [
      ("--limit", args.limit is not None),
      ("--no-stream", args.no_stream),
      ("--new-thread", args.new_thread),
      ("--rerun", bool(args.rerun)),
      ("--retry-incomplete", bool(args.retry_incomplete)),
    ]
  elif args.dry_run:
    mode = "--dry-run"
    forbidden = [
      ("--limit", args.limit is not None),
      ("--no-stream", args.no_stream),
      ("--new-thread", args.new_thread),
      ("--rerun", bool(args.rerun)),
      ("--retry-incomplete", bool(args.retry_incomplete)),
      ("--force-unlock", args.force_unlock),
    ]
  else:
    return

  conflicts = [option for option, present in forbidden if present]
  if conflicts:
    parser.error(f"{mode} 不能与以下参数同时使用：{', '.join(conflicts)}。")


def main(argv: list[str] | None = None) -> int:
  parser = build_parser()
  args = parser.parse_args(argv)
  if args.limit is not None and args.limit < 1:
    parser.error("--limit 必须大于 0。")
  validate_mode_options(parser, args)

  try:
    settings = load_settings(args.config)
    tasks = load_tasks(settings.tasks_file, settings.project_dir)

    if args.dry_run:
      print(dry_run_output(settings, tasks))
      return 0

    if args.status:
      ensure_secure_output_directory(settings)
      with RunLock(settings.output_dir / "run.lock", force=args.force_unlock):
        with prepare_store(settings, tasks) as store:
          render_chat_report(store, settings)
          print("Thread ID：", store.get_meta("thread_id") or "尚未创建")
          for line in status_lines(store):
            print(line)
          print("聊天报告：", settings.output_dir / "chat_report.html")
      return 0

    return execute_batch(
      settings,
      tasks,
      limit=args.limit,
      no_stream=args.no_stream,
      new_thread=args.new_thread,
      rerun=args.rerun,
      retry_incomplete=args.retry_incomplete,
      force_unlock=args.force_unlock,
    )
  except KeyboardInterrupt:
    print("\n已中断。当前函数会标记为 INCOMPLETE。", file=sys.stderr)
    return 130
  except sqlite3.OperationalError as exc:
    if not is_sqlite_lock_error(exc):
      raise
    print(f"错误：SQLite 状态库被锁定：{exc}", file=sys.stderr)
    return 2
  except (OSError, RuntimeError, ValueError) as exc:
    print(f"错误：{exc}", file=sys.stderr)
    return 2


if __name__ == "__main__":
  raise SystemExit(main())
