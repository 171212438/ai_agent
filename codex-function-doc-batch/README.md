# Codex 函数文档批处理

这个工具把一份函数清单自动变成连续的 Codex Turns：

```text
同一个 Thread
├── Turn 1：只处理函数 A → 永久保存 final_response
├── Turn 2：只处理函数 B → 永久保存 final_response
├── Turn 3：只处理函数 C → 永久保存 final_response
└── ...
```

每个 Turn 完成后，程序才会启动下一个 Turn。默认允许 Codex 修改项目工作区，并在每轮开始时重新启动本地 App Server、恢复同一个 Thread，再明确要求重新读取磁盘上的最新 `AGENTS.md`。

## 1. macOS 环境准备

要求：

- macOS
- Python 3.10 或更高版本
- 已经可以正常使用 Codex

先在“终端”中进入本工具目录并创建独立虚拟环境：

```bash
cd /path/to/codex-function-doc-batch
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

SDK 通常会自动复用你现有的 Codex 登录。如果尚未登录：

```bash
python3 login.py
```

如果浏览器登录不方便：

```bash
python3 login.py --device-code
```

## 2. 创建配置

复制示例文件：

```bash
cp config.example.json config.json
cp tasks.example.csv tasks.csv
```

修改 `config.json`：

```json
{
  "project_dir": "~/Projects/YourProject",
  "tasks_file": "tasks.csv",
  "output_dir": "~/Library/Application Support/codex-function-doc-batch/YourProject",
  "thread_name": "函数文档批处理",
  "model": null,
  "effort": "high",
  "stream_events": true,
  "continue_on_error": false
}
```

字段说明：

- `project_dir`：包含源码、Markdown、`AGENTS.md` 的真实项目根目录。
- `tasks_file`：CSV 或 JSON 任务清单；相对路径以 `config.json` 所在目录为基准。
- `output_dir`：宿主检查点和永久消息的专用目录，必须与 `project_dir` 完全分离；
  相对路径以 `config.json` 所在目录为基准。建议使用上例这种项目外绝对路径。
- `model`：`null` 表示沿用 Codex 当前配置；如需固定模型可填模型 ID。
- `effort`：推理强度，当前示例使用 `high`。
- `stream_events`：是否在终端显示并保存实时事件。
- `continue_on_error`：`true` 只允许越过 `FAILED_BEFORE_REQUEST` 或已收到明确终态的
  `FAILED_TERMINAL`；如果 Turn 是 `INCOMPLETE`、事件流中断或状态无法识别，程序始终停止，
  避免与未知 Turn 并发修改。

`~` 会自动展开为当前 macOS 用户目录。也可以填写完整路径，例如
`/Users/your-name/Projects/YourProject`。

首次使用的 `output_dir` 必须不存在或为空。安全升级后，旧版创建的非空输出目录因为没有
状态标记会被拒绝；请先备份旧目录，再为本版本指定一个新的空目录，程序不会自动接管或
覆盖未知目录。

请让 Codex App、终端和该脚本使用同一个 macOS 用户及相同的 `CODEX_HOME`。
如果没有专门设置 `CODEX_HOME`，保持默认的 `~/.codex` 即可。这样脚本能复用与
Codex App/CLI 相同的本地配置、Skills、MCP 和认证。

如果项目位于“桌面”“文稿”或“下载”等受 macOS 保护的目录，首次访问时系统可能要求
授权。只需给正在运行脚本的终端或 Python 访问该项目目录的权限；通常不需要开启
“完全磁盘访问权限”。把工程放在 `~/Projects` 一类普通开发目录可减少这类提示。

建议从终端启动批处理，这样更容易继承 Homebrew 安装的 `rg`、`pdftoppm` 等命令。
如果从 PyCharm 启动，请先在终端用 `which rg`、`which pdftoppm` 等命令确认工具位置，
再确保 PyCharm Run Configuration 的 `PATH` 包含这些目录。具体需要哪些外部命令，
以项目当前 `AGENTS.md` 和 Skills 为准。

## 3. 填写函数清单

用 Excel 或文本编辑器修改 `tasks.csv`。前三行是截图里已经手工完成的任务，因此示例中设置为 `false`；可以删除它们。

```csv
id,enabled,document,function,source,prompt
pwm-next-001,true,Bsp_Pwm.md,Bsp_Pwm_DeInit(),Bsp_Pwm.c,
pwm-next-002,true,Bsp_Pwm.md,Bsp_Pwm_SetDuty(),Bsp_Pwm.c,
cdd-next-001,true,Cdd_PwmWave.md,Cdd_PwmWave_DeInit(),Cdd_PwmWave.c,
```

规则：

- 一行就是一个函数和一个独立 Turn。
- `id` 必须唯一，执行后不要改。
- `document` 是目标 Markdown，相对于项目根目录。
- `function` 建议包含 `()`。
- `source` 可留空；同名静态函数应填写源文件以避免歧义。
- `prompt` 可留空；填写后会作为本轮的自定义任务原文。
- 行顺序就是执行顺序。
- 已完成任务可设为 `false`，不会再次执行。
- CSV 表头可以乱序；`enabled`、`source`、`prompt` 可省略，其中省略 `enabled` 表示默认启用。
  `id`、`function` 必须存在，并且至少包含 `document`、`target`、`prompt`、`custom_prompt`
  之一；`target`、`custom_prompt` 分别是兼容旧清单的 `document`、`prompt` 别名，不能与
  对应的规范字段同时出现在同一份 CSV 表头中。
- CSV 的未知、空白或重复表头，以及比表头多列或少列的数据行，会在创建输出目录和启动
  Codex 前直接拒绝，避免字段拼写错误把原本禁用的任务按默认值启用。

也支持 JSON：

```json
[
  {
    "id": "pwm-next-001",
    "enabled": true,
    "document": "Bsp_Pwm.md",
    "function": "Bsp_Pwm_DeInit()",
    "source": "Bsp_Pwm.c"
  }
]
```

JSON 任务可使用顶层数组，或精确的 `{"tasks": [...]}` 包装对象。任务字段与 CSV 相同：
`id`、`function` 以及所有出现的文本字段必须是字符串，`enabled` 若出现必须是 JSON
布尔值；只有省略 `enabled` 才表示默认启用。显式 `null`、未知或重复字段、非标准
`NaN`/`Infinity`、规范字段与兼容别名并存，以及包装对象中的额外字段都会被拒绝。
空数组仍是合法的空任务清单。

## 4. 先检查，不执行

```bash
python3 run_batch.py --config config.json --dry-run
```

程序会检查：

- 项目路径是否正确
- 任务 ID 是否重复
- 目标路径是否越出项目目录
- 实际将发送给 Codex 的逐函数 Prompt

建议先提交或备份当前 Git 修改。

## 5. 先试两个函数

```bash
python3 run_batch.py --config config.json --limit 2
```

确认结果符合 `AGENTS.md` 后，再执行全部剩余任务：

```bash
python3 run_batch.py --config config.json
```

如果几十个函数会连续执行很久，可用 macOS 自带的 `caffeinate` 防止机器因空闲进入睡眠：

```bash
caffeinate -i python3 run_batch.py --config config.json
```

如果使用 PyCharm for macOS，可在 Run Configuration 中设置：

- Script path：`run_batch.py`
- Parameters：`--config config.json`
- Working directory：本工具目录
- Python interpreter：本工具目录下的 `.venv/bin/python`

## 6. 输出在哪里

示例输出到目标项目之外的私有控制目录：

```text
~/Library/Application Support/codex-function-doc-batch/YourProject/
├── .codex-function-doc-batch-state.json
├── run.sqlite3
├── chat_report.html
├── responses/
│   ├── v2-task-<task-id-sha256>-a1.md
│   └── v2-task-<task-id-sha256>-a2.md
├── prompts/
└── events/
```

其中：

- 每个 `responses/*.md` 是一个函数永久、独立的最终消息。
- 新工件文件名由完整 `task_id` 的 SHA-256 与 Attempt 编号确定，不依赖任务排序或有损
  slug；旧 Attempt 仍按数据库中已保存的原路径读取，不自动改名。
- `chat_report.html` 把每个 Prompt 和最终回复显示成独立聊天气泡。
- `events/*.jsonl` 保存该 Turn 的结构化工作事件。
- `run.sqlite3` 保存 `thread_id`、`turn_id`、状态、时间和检查点。

打开已有状态库时会先只读核对表结构与 `schema_version`；只接受当前版本，未来版、旧版、
非法版本或有状态但缺版本的数据库都会在建表前被拒绝且不会被重新标记。全新库以及结构可
识别、所有表均为空的 partial bootstrap 才允许初始化。建表、`schema_version` 与
`created_at` 在同一短事务中提交，失败时一起回滚，已有的 `created_at` 不会被覆盖。状态库
会再以短事务绑定 `project_dir`，每次任务清单同步也会整体提交或整体回滚。若状态库
缺少 `project_dir` 却已有任务、Attempt 或其他项目元数据，会按损坏状态拒绝自动绑定；
已有绑定也必须是与当前项目匹配的绝对路径。
`config_path` 与 `tasks_file` 只有在任务同步、事件恢复和聊天报告生成均成功后才会一起更新；
任务同步中途失败不会保留前序部分写入，准备阶段异常也会关闭数据库连接并保留上一次来源
路径。单项元数据写入或删除也只管理自己的短事务；若连接已有事务则拒绝执行，避免意外提交
调用方状态。事件日志恢复仍按 Attempt 逐项安全提交。响应 Markdown 会先在由 `output_dir`
目录描述符锚定的父目录中写入并 `fsync` 私有候选文件；收尾事务的 Task/Attempt CAS 都成功
后、SQLite 提交前才通过同一父目录描述符原子发布并同步父目录。CAS 冲突不会创建或覆盖最终
响应，发布失败也会回滚两条状态更新。

在 macOS 中可直接打开聊天气泡报告：

```bash
open "$HOME/Library/Application Support/codex-function-doc-batch/YourProject/chat_report.html"
```

这些文件不会自动插入你当前打开的 Codex App 对话界面；它们是脚本自己的永久记录。SDK 创建的仍然是真实 Thread/Turns，Codex 会在真实项目里完成读取、命令、Skills、PDF/源码核对和 Markdown 修改，但 App 的“审核/撤销”按钮需要 App 自身界面，脚本不会复制。

## 7. 中断后继续

已经成功的函数永远跳过。正常停止后，再次执行同一命令即可从下一个函数继续：

```bash
python3 run_batch.py --config config.json
```

新 Attempt 首先持久化为 `TURN_NOT_REQUESTED`，然后在调用 `thread.turn()` 前切换为
`TURN_START_REQUESTED`。只有前一个状态能够由本地记录证明请求尚未发出；如果进程在该阶段
退出，可以显式重试：

```bash
python3 run_batch.py --config config.json --retry-incomplete <task-id>
```

`--retry-incomplete` 只接受 task 和最新 Attempt 均为 `TURN_NOT_REQUESTED`、
`thread_id/turn_id` 均为空、且不存在其他未完成 Attempt 的任务。安全重试会把旧 Attempt
记录为 `ABANDONED_BEFORE_REQUEST`，不会与历史状态混淆。

`STARTING`、`FAILED` 和 `ABANDONED` 是旧版本遗留的歧义状态：旧代码可能在请求已受理、
但尚未写 Turn ID 或终态时留下它们。因此这些历史状态与 `TURN_START_REQUESTED`、
`RUNNING`、`INCOMPLETE` 都不能用 `--retry-incomplete` 放行。程序会先尝试从事件日志恢复
匹配的已知终态；没有终态证据时将保持停止，也不会仅凭人工检查目标 Markdown 就启动新 Turn。

实时请求返回后，`handle.id` 必须是无首尾空白的非空字符串；流式 `turn/started`、
`turn/completed` 中出现的 ID，以及非流式 `handle.run()` 结果 ID，都必须与已经写入检查点的
Turn ID 完全一致。ID 缺失或不一致时不会采信其终态或最终响应：任务记录为 `INCOMPLETE`，
保留原检查点 ID（尚未取得有效 Handle ID 时保持为空），并无条件停止后续任务。

从历史事件 JSONL 恢复时，每个非空行都必须是带非空 `method` 的 JSON 对象；
`turn/started`（若存在）、`item/completed` 和 `turn/completed` 的相关 payload 也必须是对象。
`turn/started` 与唯一且位于日志末尾的 `turn/completed` 必须显式携带与数据库完全一致的
字符串 Turn ID，禁止用缺失 ID 回退数据库值或把数字强转成字符串。JSON 语法、UTF-8、
结构、ID、终态 status 或事件顺序任一不符合要求时，整份日志保持不可恢复，不推进数据库，
也不创建或覆盖响应文件。未知 `method` 在终态前仍会被忽略，以兼容未来 SDK 事件；畸形的
token usage 遥测也不会阻止通过身份校验的终态恢复。`batch/nonStreamingMode` 不是未知事件：
它明确表示该文件没有完整流式证据，因此即使文件后来被追加终态，也始终禁止自动恢复。

新版本不再写裸 `FAILED`：已经创建 `TURN_NOT_REQUESTED` Attempt、尚未请求 Turn，且由
普通异常分支成功收口的本地失败记录为 `FAILED_BEFORE_REQUEST`；已收到明确失败终态的
Turn 记录为 `FAILED_TERMINAL`。这两个状态具有明确来源，可以在普通续跑或 `--rerun` 时
安全重试。Attempt 创建前的静态校验或 Prompt 私有候选写入失败不会改写任务状态；如果
Attempt 已成功预留路径、但候选发布失败，则保留 `TURN_NOT_REQUESTED` 且不会请求 Turn，
须对账后使用 `--retry-incomplete` 安全重试。

`--no-stream` 模式不保存完整事件流，因此请求发出后的异常中断通常无法从 JSONL 自动恢复，
也不能使用 `--retry-incomplete`；程序会保守停止。

要主动重新执行一个可安全重跑的任务：

```bash
python3 run_batch.py --config config.json --rerun <task-id>
```

`--rerun` 只接受 `SUCCEEDED`、`FAILED_BEFORE_REQUEST` 或 `FAILED_TERMINAL`。只要任务表
或任意 Attempt 仍是历史 `STARTING/FAILED/ABANDONED`、`TURN_NOT_REQUESTED`、
`TURN_START_REQUESTED`、`RUNNING` 或 `INCOMPLETE`，程序会在处理 `--rerun` 前停止；
不得用它绕过未完成状态保护。

同一命令中的多个 `--rerun` 和 `--retry-incomplete` 会先分别保序去重，再在一个
`BEGIN IMMEDIATE` 事务中整体校验和修改。任一任务不存在、状态不允许，或仍有未被安全
重试覆盖的未完成任务时，整个批次都不会改变 task/attempt 状态。同一任务不能同时出现在
两类参数中。

要让剩余任务改用一个全新 Thread：

```bash
python3 run_batch.py --config config.json --new-thread
```

`--new-thread` 不会预先删除现有 `thread_id`。只有远端新 Thread 成功返回有效的新 ID 后，
程序才会在一个短事务中直接用新 ID 替换旧 ID，并且替换发生在首个 Turn 请求之前。没有
待处理任务、SDK 加载失败或新 Thread 创建失败时，旧 ID 保持不变；替换时若发现旧 ID 已
被异常改写，整个批次会停止且不会发出 Turn。

查看状态：

```bash
python3 run_batch.py --config config.json --status
```

如果状态命令被旧版 `run.lock` 阻止，并且旧 PID 已经退出，可在不启动任何 Turn 的前提下
迁移旧锁并查看状态：

```bash
python3 run_batch.py --config config.json --status --force-unlock
```

活跃 PID、权限不明或内容损坏的旧锁仍会被拒绝。

命令模式会拒绝可能被静默忽略的参数组合：

- `--dry-run` 与 `--status` 互斥。
- `--dry-run` 不能与 `--limit`、`--no-stream`、`--new-thread`、`--rerun`、
  `--retry-incomplete` 或 `--force-unlock` 同时使用。
- `--status` 不能与上述执行参数同时使用，但允许安全迁移旧锁所需的
  `--status --force-unlock`。

非法组合会在读取配置或访问状态目录前以退出码 `2` 终止。

## 8. 安全注意事项

- 不要同时启动两个批处理进程。
- 批处理期间不要让另一个 Codex 或人工同时编辑同一 Markdown。
- 默认使用 `Sandbox.workspace_write`，不是 Full Access。
- `output_dir` 必须位于项目工作区之外，程序会拒绝项目内目录、项目父目录、权限不是
  `0700` 的目录，以及缺少状态标记的非空目录。控制文件使用 `0600` 权限创建。
- `run.sqlite3` 会先用 `O_NOFOLLOW` 打开或创建主库保护描述符，并在 `sqlite3.connect()`
  前后、状态事务边界和关闭前复核普通文件、当前属主、单链接及路径 inode 身份；检查时可见
  的符号链接、额外硬链接或路径替换会停止运行。需要备份时应复制数据库，不要为主库创建
  硬链接。
- `prompts/`、`events/`、`responses/` 工件路径必须保持在各自固定子目录；实时运行和恢复
  都会重新检查路径，拒绝任何已存在的符号链接组件（即使链接目标仍在 `output_dir` 内）、
  非普通文件叶子以及带额外硬链接的叶子，避免把其他状态文件当成工件读取、截断或替换。
  缺失的工件目录由首次实际写入延迟创建。
- 恢复事件时会从 `output_dir` 的目录描述符开始，以 `dir_fd + O_NOFOLLOW` 逐级打开路径，
  并只读取经首次 `fstat` 确认为普通文件且当时链接数为 `1` 的描述符；因而校验后把工件
  父组件或叶子换成符号链接不会重定向本次读取。
- 实时流式事件文件会在创建 Codex SDK 实例和请求 Turn 之前打开。写入从 `output_dir`
  描述符开始，以 `dir_fd + O_NOFOLLOW` 逐级打开或创建父目录，并把当前用户拥有的既有
  父目录权限收紧为 `0700`；叶子文件打开时不带 `O_TRUNC`，只有在描述符通过普通文件、
  当前属主、单链接检查，并且同一父目录下的路径仍指向相同设备和 inode 后，才会截断并
  设置 `0600`。普通动态打开异常会在 Codex/Turn 前失败；状态库能够正常收口时记录为
  `FAILED_BEFORE_REQUEST`。平台缺少 `O_NOFOLLOW`、`O_DIRECTORY` 或所需 `dir_fd` 能力时
  同样会在 Codex/Turn 前拒绝运行，不做不安全降级。
- 状态标记、Prompt、非流式事件、最终响应和聊天报告的原子写入也从 `output_dir` 描述符
  开始，以 `dir_fd + O_NOFOLLOW` 打开或创建并持有每一级父目录。私有候选文件通过
  `O_EXCL + O_NOFOLLOW` 以 `0600` 创建；发布前会重新核对根目录、父目录和候选文件的
  设备/inode、文件类型、属主及链接数，再在已打开的同一父目录内执行描述符相对原子替换。
  如果父路径在写入期间被重命名或换成符号链接，发布会拒绝，而不会沿新路径写到目录外。
- 上述描述符协议不能证明事件内容可信，也不能防止同一用户在最后一次链接检查之后新增
  硬链接、直接修改已经打开的 inode，或在根目录描述符打开前换入另一个同属主的真实
  `0700` 目录。标准库 `sqlite3` 也不暴露其实际主库文件描述符，因此无法彻底排除同一用户
  在两次检查之间完成路径替换再恢复的 ABA 竞态。状态标记的既有文件读取仍是路径式校验和
  读取。因此这些检查不能替代私有 `0700` 输出目录和 `run.lock`，也不能把 `output_dir` 与
  其他程序共用。
- 工作区外文件、网络或受限命令仍可能被 Sandbox/审批策略拒绝。
- 失败后默认停止；检查原因后再继续。
- 输出可能包含代码、命令和文档信息，不建议提交到公共仓库。
- 不要把 `output_dir` 设为项目目录或其父目录，也不要与其他程序共用该目录。
- 建议把项目和 `output_dir` 放在本机磁盘；不要把运行中的 SQLite、锁文件和事件日志放在
  iCloud Drive、Dropbox、SMB/NFS 等同步或网络目录。
- macOS 常见磁盘格式默认不区分文件名大小写；`tasks.csv` 中的路径仍应严格使用 Git
  记录的真实大小写。
- `run.lock` 是持久的锁记录。新版本在整个批处理期间持有同一文件描述符的
  `flock`；正常退出或进程崩溃都会由内核释放锁，文件本身无需删除。
- 状态库通过结构验收后才切换为 WAL；切换遇到 SQLite 的瞬时 `BUSY/LOCKED` 会在有限
  时间内重试并严格校验结果。该重试只保护 WAL 初始化，不能替代 `run.lock`，也不表示
  可以绕过 CLI 并发调用 `Store`、`prepare_store` 或其他批处理状态变更。
- 最终冒泡到 CLI 的 SQLite `BUSY/LOCKED` 会输出单行状态库锁错误并以退出码 `2` 结束，
  不显示 traceback；非锁类 SQLite 错误不会被这个分支吞掉。
- 任务执行期间的本地 SQLite 访问异常会原样冒泡并立即停止批处理；程序不会把它改记为
  Turn 失败或尝试二次写库，状态库保留最后一次成功提交的检查点。
- `TURN_NOT_REQUESTED → TURN_START_REQUESTED → RUNNING` 的检查点更新使用 CAS 条件；进入
  `RUNNING` 时还要求旧 `turn_id` 为空，禁止覆盖其他路径已经写入的身份。若本地状态已变化，
  整次更新会回滚并以专用冲突异常停止批处理，不会再改写成 Turn 失败状态。
- Attempt 收尾会分别绑定读取到的 Task/Attempt 来源状态、预期 Turn ID，并要求该 Attempt
  仍是 `latest_attempt`；任一条件变化都会回滚两条更新并停止批处理，不覆盖竞争路径的结果。
- 收尾响应先写入由 `output_dir` 目录描述符锚定的父目录中的私有候选文件；只有上述 CAS
  全部成功后，事务才在该父目录描述符内原子发布候选并提交。
  文件系统与 SQLite 之间没有分布式事务；若进程恰在文件替换成功、数据库提交前退出，可能
  留下数据库尚未决但响应已可见的保守状态。程序不会把响应文件本身当作终态证据，流式事件
  完整时可在下次启动重新恢复并收口。
- 新 Thread 创建后若发现持久化 `thread_id` 已被其他路径改变，会保留该路径的 Thread 选择
  以及当前 `TURN_NOT_REQUESTED` Attempt，并立即停止；普通运行不会自动重试，须先对账后
  使用 `--retry-incomplete`，必要时同时指定 `--new-thread`。
- `--force-unlock` 只用于迁移旧版本创建的锁：程序仅在旧锁 PID 已被系统明确判定为
  不存在时原地升级锁协议。PID 仍存活、权限不足或锁内容损坏时都会拒绝启动，不能用该
  参数强行覆盖活跃锁。
- 不要混用修复前后的脚本副本；旧版本不持有 `flock`，其 `--force-unlock` 仍可能破坏
  新版本的运行锁。锁目录仍须位于本机磁盘，不能放在 SMB/NFS 或同步盘。

## 9. 运行测试

测试使用模拟 Codex，不会连接服务、不会修改你的项目：

```bash
python3 -m unittest discover -s tests -v
```
