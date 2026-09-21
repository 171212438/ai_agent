from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from openai_codex import Codex, Sandbox


# Turn 0 输出的函数清单必须符合这个 JSON 结构
FUNCTION_LIST_SCHEMA = {
  "type": "object",
  "properties": {
    "functions": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "file": {"type": "string"},
          "line": {"type": "integer"},
          "kind": {"type": "string"},
        },
        "required": ["name", "file", "line", "kind"],
        "additionalProperties": False,
      },
    }
  },
  "required": ["functions"],
  "additionalProperties": False,
}

def parse_arguments() -> argparse.Namespace:
  parser = argparse.ArgumentParser(description="使用 Codex SDK 逐个分析 C/H 文件中的函数")
  parser.add_argument("--project", required=True, help="源码工程根目录", )
  parser.add_argument("--files", nargs="+", required=True, help="需要分析的 C/H 文件，相对于工程根目录", )
  parser.add_argument("--output", default="./results", help="分析结果保存目录", )
  parser.add_argument("--limit", type=int, default=None, help="只分析前 N 个函数，用于首次验证", )

  return parser.parse_args()

def validate_files(project: Path, raw_files: list[str]) -> list[str]:
  """校验文件存在，并确保文件位于工程目录内。"""

  relative_files: list[str] = []

  for raw_file in raw_files:
    candidate = Path(raw_file)

    if not candidate.is_absolute():
      candidate = project / candidate

    candidate = candidate.resolve()

    try:
      relative = candidate.relative_to(project)
    except ValueError as exc:
      raise SystemExit(f"文件不在工程目录内：{candidate}") from exc

    if not candidate.is_file():
      raise SystemExit(f"文件不存在：{candidate}")

    relative_files.append(relative.as_posix())

  return relative_files

def safe_filename(function_name: str) -> str:
  """将函数名转换为安全的文件名。"""

  return re.sub(r"[^A-Za-z0-9_.-]+", "_", function_name, )

def save_json(path: Path, data: Any) -> None:
  path.write_text(json.dumps(data, ensure_ascii=False, indent=2, ), encoding="utf-8", )


def build_inventory_prompt(target_files: list[str]) -> str:
    file_list = "\n".join(
        f"- {file_name}"
        for file_name in target_files
    )

    return f"""
请读取以下 C/H 文件：

{file_list}

任务：建立完整的函数定义清单。

要求：

1. 包含 .c 文件中具有函数体的函数。
2. 包含 .h 文件中的 static inline 函数。
3. 排除只有声明、没有函数体的函数原型。
4. 排除函数式宏。
5. file 使用相对于当前工程目录的路径。
6. line 使用函数定义起始行号，行号从 1 开始。
7. kind 可填写 external、static、inline 或 static_inline。
8. 按以下顺序排列：
   - 对外接口函数；
   - 主要业务函数；
   - 内部 static 辅助函数；
   - static inline 函数。
9. 只返回符合指定 Schema 的 JSON，不要附加解释。
""".strip()


def build_analysis_prompt(function: dict[str, Any]) -> str:
    return f"""
请只分析下面这一个函数：

- 函数名：{function["name"]}
- 所在文件：{function["file"]}
- 起始行：{function["line"]}
- 函数类型：{function["kind"]}

你可以读取相关类型定义、宏、全局变量以及直接调用的函数，
但本次只输出 {function["name"]} 的完整分析。
不要顺便展开分析其他函数。

请使用以下结构：

# 函数：{function["name"]}

## 1. 函数作用

## 2. 输入参数

## 3. 返回值

## 4. 前置条件

## 5. 执行流程

## 6. 调用的其他函数

## 7. 访问的全局变量和静态变量

## 8. 关键宏、类型和配置依赖

## 9. 错误处理和异常路径

## 10. 并发、重入和中断安全性

## 11. 潜在缺陷与风险

## 12. 建议测试项

要求：

1. 结论必须基于实际源码。
2. 重要结论尽量标明文件和行号。
3. 证据不足时明确说明，不要猜测。
4. 最终回答只包含该函数的分析结果。
""".strip()

def main() -> None:
  args = parse_arguments()

  project = Path(args.project).resolve()
  output = Path(args.output).resolve()

  if not project.is_dir():
    raise SystemExit(f"工程目录不存在：{project}")

  output.mkdir(parents=True, exist_ok=True)

  target_files = validate_files(project, args.files, )

  with Codex() as codex:
    # 整个 C/H 分析任务只创建一个 Thread
    thread = codex.thread_start(cwd=str(project), sandbox=Sandbox.read_only, )

    print(f"Thread ID：{thread.id}")

    # Turn 0：生成函数队列
    print("\n正在读取 C/H 文件并生成函数队列……")

    inventory_result = thread.run(build_inventory_prompt(target_files), output_schema=FUNCTION_LIST_SCHEMA, sandbox=Sandbox.read_only, )

    if inventory_result.error is not None:
      raise RuntimeError(f"函数清单生成失败：{inventory_result.error}")

    if not inventory_result.final_response:
      raise RuntimeError("函数清单 Turn 没有返回 final_response")

    inventory = json.loads(inventory_result.final_response)

    functions = inventory["functions"]

    save_json(output / "function_inventory.json", {"thread_id": thread.id, "inventory_turn_id": inventory_result.id, "target_files": target_files, "functions": functions, }, )

    print(f"共识别到 {len(functions)} 个函数")

    if args.limit is not None:
      functions = functions[: args.limit]
      print(f"本次只分析前 {len(functions)} 个函数")

    completed_records: list[dict[str, Any]] = []

    # 每循环一次，就创建一个新的独立 Turn
    for index, function in enumerate(functions, start=1):
      function_name = function["name"]

      print(f"\n[{index}/{len(functions)}] "
            f"开始分析：{function_name}")

      result = thread.run(build_analysis_prompt(function), sandbox=Sandbox.read_only, )

      if result.error is not None:
        print(f"函数 {function_name} 分析失败："
              f"{result.error}")
        break

      if not result.final_response:
        print(f"函数 {function_name} "
              "没有返回 final_response")
        break

      result_file = output / (f"{index:03d}_"
                              f"{safe_filename(function_name)}.md")

      result_file.write_text((f"# {function_name}\n\n"
                              f"- Thread ID：`{thread.id}`\n"
                              f"- Turn ID：`{result.id}`\n"
                              f"- 源文件：`{function['file']}`\n"
                              f"- 起始行：`{function['line']}`\n\n"
                              f"{result.final_response}\n"), encoding="utf-8", )

      completed_records.append({"index": index, "function": function_name, "thread_id": thread.id, "turn_id": result.id, "result_file": str(result_file), "status": str(result.status), })

      # 每完成一个函数立即更新索引
      save_json(output / "run_index.json", {"thread_id": thread.id, "completed": completed_records, }, )

      print(f"完成：{function_name}\n"
            f"Turn ID：{result.id}\n"
            f"结果：{result_file}")

      # 当前 final_response 直接显示在终端
      print("\n" + result.final_response)

    print("\n本次逐函数分析结束")

if __name__ == "__main__":
  main()
