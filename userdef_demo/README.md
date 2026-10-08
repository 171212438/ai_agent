# UserDef 最小演示包

这个示例用于观察：如何通过 `python -m userdef run demo.py` 启动一个 Python 包，并由它执行指定脚本。

它仅演示命令行入口和脚本执行，不包含 Web 服务或页面功能。

## 运行要求

- Python 3.10 或以上，支持 Python 3.13。
- 运行示例不需要安装第三方依赖。
- macOS 上如果没有 `python` 命令，请把下面命令中的 `python` 替换为 `python3`。

## 直接运行

解压后，在终端进入项目根目录：

```bash
cd userdef_demo
```

执行演示脚本：

```bash
python -m userdef run demo.py
```

预期输出：

```text
你好，UserDef！
__name__ = __main__
```

在项目根目录运行时，不需要先安装这个包。

查看帮助：

```bash
python -m userdef --help
```

```bash
python -m userdef run --help
```

## 文件职责

| 文件 | 职责 |
|---|---|
| `userdef/__init__.py` | 提供 `greet(name)`，返回 `你好，{name}！`。 |
| `userdef/__main__.py` | 接收 `python -m userdef` 入口，调用 `cli.main()`。 |
| `userdef/cli.py` | 解析 `run` 命令和脚本路径，然后执行脚本。 |
| `demo.py` | 调用 `greet()` 并显示当前 `__name__`，验证演示功能。 |
| `pyproject.toml` | 描述包信息，支持可选的本地安装。 |
| `README.md` | 运行和调试说明。 |

命令中的 `-m userdef` 由 Python 处理；后面的 `run demo.py` 由 `userdef` 自己解析。`run` 是这个示例定义的命令。

脚本通过 `runpy.run_path(..., run_name="__main__")` 在当前 Python 进程内执行，因此 `demo.py` 中的 `__name__` 为 `"__main__"`。这个最小版本只接收脚本路径，暂不转发额外的脚本参数。

## 在 PyCharm 中调试

用 PyCharm 打开 `userdef_demo` 目录，选择 Python 3.10 或以上解释器。新增一个 Python 运行配置，并填写：

| 配置项 | 值 |
|---|---|
| 运行目标类型 | 模块名称 / Module name |
| 模块名称 | `userdef` |
| 参数 | `run demo.py` |
| 工作目录 | 解压后的 `userdef_demo` 根目录 |

建议先在两个位置设置断点：

1. `userdef/__main__.py` 中调用 `main()` 的一行。
2. `userdef/cli.py` 中调用 `runpy.run_path(...)` 的一行。

启动调试后，观察 `cli.py` 中解析得到的脚本路径；执行到 `demo.py` 后，再观察 `__name__`。如果 IDE 默认跳过库代码，可直接在 `demo.py` 中增加断点。

## 可选：安装到当前 Python 环境

如果希望在其他目录也能找到 `userdef`，可在项目根目录执行：

```bash
python -m pip install -e .
```

这是可编辑安装：修改本地源码后，下次运行即可使用修改后的内容。安装过程可能需要联网获取 `setuptools`、`wheel` 构建依赖；直接运行前面的示例不需要这一步。

安装后，脚本路径仍按当前工作目录解析；在其他目录运行时，请提供正确的相对路径或绝对路径。

## 错误与清理

- 指定的脚本不存在或路径不正确时，命令会报错。先检查终端的当前目录和脚本路径。
- 脚本执行中发生异常时，会保留 Python traceback，便于定位出错文件和行号。
- 未安装时，删除 `userdef_demo` 目录即可移除示例源码。
- 如果执行过安装，请先在同一个 Python 环境中卸载，再按需删除目录：

```bash
python -m pip uninstall userdef
```

## 验证记录

本次在 Python 3.12.14 中实际验证了直接运行、帮助输出、错误命令和错误路径提示、本地安装后从其他目录启动，以及包含空格和中文的脚本路径、脚本同目录模块导入。Python 3.13 未在本次环境中实际运行验证。

## 对照资料

- [Python：包中的 __main__.py](https://docs.python.org/3.13/library/__main__.html)
- [Python：runpy.run_path](https://docs.python.org/3.13/library/runpy.html#runpy.run_path)
- [Python Packaging User Guide：项目打包](https://packaging.python.org/en/latest/tutorials/packaging-projects/)
