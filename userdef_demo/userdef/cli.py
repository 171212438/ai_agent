"""解析 run 命令，并在当前 Python 进程中执行目标脚本。"""

import argparse
from pathlib import Path
import runpy
import sys


def main() -> None:
    """处理 python -m userdef run demo.py。"""
    parser = argparse.ArgumentParser(
        prog="python -m userdef",
        description="UserDef 最小演示：执行一个 Python 脚本。",
    )
    parser.add_argument("command", choices=["run"], help="执行脚本")
    parser.add_argument("script", type=Path, help="待执行脚本的路径")
    args = parser.parse_args()

    # 相对路径以启动命令时的工作目录为基准。
    script = args.script.resolve()
    if not script.is_file():
        parser.error(f"脚本文件不存在或不是普通文件：{args.script}")

    original_argv = sys.argv
    original_path = sys.path[:]
    try:
        # 目标脚本只看到自己的路径；本演示不转发额外脚本参数。
        sys.argv = [str(script)]
        # 允许目标脚本导入位于它同一目录下的模块。
        sys.path.insert(0, str(script.parent))
        runpy.run_path(str(script), run_name="__main__")
    finally:
        # 即使目标脚本报错，也恢复这里调整过的进程状态。
        sys.argv = original_argv
        sys.path[:] = original_path
