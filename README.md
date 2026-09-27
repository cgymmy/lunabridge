# luna-bridge — 本地命令行 ↔ JumpServer 网页终端 交互桥

通过浏览器自动化的方式，让本地命令行（或 Claude Code 等 AI 编程助手）可以
直接向 JumpServer Luna 网页终端发送命令并取回输出，无需手工复制粘贴。

```
本地终端 / Claude Code / 脚本
        │  python luna_cli.py -c "命令"
        ▼
Playwright 驱动 Chromium（复用已保存的登录会话）
        │  自动登录检测 → 自动点击"连接" → 终端就绪
        ▼
JumpServer Luna 网页终端（iframe 内 xterm.js）
        │  keyboard.type 输入命令 / CDP 捕获 WS 下行帧输出
        ▼
堡垒机 KoKo → 目标服务器
```

---

## 一、环境要求与安装

| 依赖 | 说明 |
|---|---|
| Windows / Linux / macOS | 任意支持 Playwright 的桌面系统 |
| Python ≥ 3.9 | 推荐 3.10+ |
| Playwright + Chromium | 见下方安装命令 |

```bash
# 1. 安装 Playwright Python 库
pip install playwright

# 2. 下载 Chromium 内核（约 150MB，只需一次）
python -m playwright install chromium
```

> 本工具在 Windows 11 + Python 3.13 + Playwright 1.59 + JumpServer v4.10.18
> 环境验证通过。目标机浏览器内核版本如有差异，升级 Playwright 即可。

---

## 二、快速开始

### 第 1 步：配置目标资产

复制示例配置并填写自己的 JumpServer 地址、资产 UUID、组织 UUID 和
WebSFTP 导航路径：

```bash
cp luna_config.example.json luna_config.json
```

`luna_config.json` 已加入 `.gitignore`，不会被提交。也可以通过
`LUNA_BASE_URL`、`LUNA_ASSET_ID`、`LUNA_ORG_ID`、`LUNA_SFTP_NAV`
环境变量提供配置。

### 第 2 步：首次登录（只需一次）

```bash
python luna_cli.py --login
```

- 会弹出 Chromium 窗口，请在窗口里手动完成登录
  （用户名 + 密码 + **图形验证码**——验证码无法自动绕过，这是唯一需要人工的环节）
- 登录成功后脚本会自动检测终端、保存会话到 `storage_state.json` 并退出
- 如果登录后停留在"连接方式"对话框，脚本会自动勾选"下次自动登录"并点击"连接"；
  勾选后以后每次运行都会自动连接，不再弹对话框

### 第 3 步：日常使用

```bash
# ① 持久在线模式：像 SSH 一样使用；断线时会自动重连/刷新恢复
python luna_cli.py

# ② 单条命令：执行一条命令，打印输出后退出
python luna_cli.py -c "df -h"

# ③ 静默模式：无浏览器窗口，适合写进脚本 / 定时任务 / CI
python luna_cli.py --headless -c "nvidia-smi"

# ④ 上传文件：默认走 JumpServer 原生 WebSFTP，并自动校验 SHA-256
python luna_cli.py --headless --upload ./model.tar.gz /tmp/model.tar.gz

# WebSFTP 不可用时会自动回退；也可以显式指定上传通道
python luna_cli.py --upload ./data.bin /tmp/data.bin --upload-method sftp
python luna_cli.py --upload ./data.bin /tmp/data.bin --upload-method terminal

# ⑤ 下载文件：默认优先 WebSFTP，失败时自动回退到终端通道
python luna_cli.py --headless --download /tmp/result.tar.gz ./result.tar.gz
python luna_cli.py --headless --download /tmp/result.tar.gz ./result.tar.gz --download-method sftp
```

连续执行多条命令时，推荐直接运行 `python luna_cli.py` 保持浏览器和终端长连接；
反复使用 `-c` 会为每条命令重新启动 Chromium，连接延迟会更高。

| 参数 | 说明 |
|---|---|
| `--login` | 首次手动登录并保存会话 |
| `-c "命令"` | 执行单条命令后退出 |
| `--headless` | 隐藏浏览器窗口（配合 `-c` 使用） |
| `--wait 秒数` | 命令执行后等待输出静默的时长（默认 1.2 秒，长命令可加大） |
| `--upload 本地文件 远端路径` | 上传文件；默认优先使用原生 WebSFTP |
| `--upload-method auto\|sftp\|terminal` | 上传通道；`auto` 失败时回退到终端流式传输 |
| `--upload-rate MiB/s` | 仅限制终端回退通道的入队速率（默认 16，`0` 不限速） |
| `--download 远端路径 本地文件` | 从目标资产下载文件 |
| `--download-method auto\|sftp\|terminal` | 下载通道；`auto` 失败时回退到终端 Base64 |
| `--no-log` | 本次运行不写入终端日志 |

**命令包含特殊字符时**（`$`、`"`、`|`、`>` 等）：
- PowerShell：用单引号包裹整个命令
  ```powershell
  python luna_cli.py -c 'echo $HOME && ls | head -5'
  ```
- Git Bash / Linux：命令字符串会原样敲进终端，支持 `&&`、管道、重定向、引号嵌套

### 第 4 步：Windows 快捷方式（可选）

双击或命令行使用 `run.bat`，省去输入 python 前缀：

```bat
run.bat                    :: 交互模式
run.bat -c "df -h"         :: 单条命令
run.bat --headless -c "uptime"
```

---

## 三、让 Claude Code / AI 助手使用

把本文件夹放进你的项目后，在 Claude Code 对话中直接说：

> "帮我在目标服务器上跑 nvidia-smi"

Claude 会执行 `python luna-bridge/luna_cli.py -c "nvidia-smi"` 并把结果展示给你。
在脚本中调用时读取 `last_output.txt`（每次 `-c` 运行的输出都会保存到这里）
或直接捕获标准输出。

---

## 四、工作原理（简述）

1. **会话复用**：首次登录的 Cookie/Token 保存在 `storage_state.json`，
   之后每次启动直接复用；会话过期时脚本会检测到登录页并提示重新登录。
2. **自动连接**：Luna 登录后弹"连接 - 资产名"对话框，脚本自动点击"连接"；
   由于已勾选"下次自动登录"，后续运行将自动直连终端。
3. **终端定位**：Luna 把终端渲染在 `/koko/connect/?token=...` 的
   **iframe** 中，脚本遍历所有标签页与 frame 定位 xterm。
4. **输出捕获**：通过 CDP (`Network.webSocketFrameReceived`) 捕获
   KoKo 终端 WebSocket 的**二进制帧**（终端输出直接以二进制传输），
   做增量 UTF-8 解码还原输出。
5. **命令输入**：用 `keyboard.type` 向 xterm 的隐藏文本域输入真实键盘事件
   （xterm.js 只响应 keydown，`insert_text` 不生效）。
6. **关键实现细节**：所有等待使用 Playwright 的 `wait_for_timeout` 而非
   `time.sleep` —— 同步 API 中 `time.sleep` 会阻塞事件分发线程，
   导致 WS 帧延迟数分钟才到达（踩过的大坑）。
7. **快速上传**：优先使用 JumpServer 自带的 WebSFTP/elFinder HTTP 上传，
   文件不再经过 Base64，因此没有 33% 体积膨胀，也不会挤占终端输入队列。
   为避免覆盖同名文件，本地使用同卷硬链接生成唯一暂存名；远端上传到
   `/tmp` 后校验大小和 SHA-256，校验通过才移动到最终路径。
8. **快速下载**：优先把目标文件硬链接或暂存到远端 `/tmp`，再通过
   WebSFTP 流式下载到同目录临时文件；大小和 SHA-256 校验通过后才原子替换
   本地目标。WebSFTP 不可用时自动回退到终端 Base64 下载。
9. **兼容回退**：WebSFTP 上传不可用时，使用固定内存的流式 Base64 接收器。
   回退模式关闭终端回显、持续发送分块，并在替换目标文件前完成同样的
   大小与哈希校验。
10. **断线恢复**：只有终端 WebSocket 握手成功才标记为在线；监听关闭、
   帧错误和页面导航。交互模式会先等待 KoKo 原生重连，仍失败时自动刷新
   Luna 并重新定位 xterm。控制台输入在独立线程读取，主线程始终派发网络事件。
11. **日志控制**：`luna_term.log` 默认达到 20 MiB 后轮转，保留两份备份；
   处理敏感命令或不需要审计时可以使用 `--no-log`。

---

## 五、常见问题

| 问题 | 处理 |
|---|---|
| 为什么每次 `-c` 后显示会话断开 | `-c` 是一次性命令，拿到输出后会主动关闭浏览器，属于预期行为；需要持久在线请直接运行 `python luna_cli.py` |
| 交互模式因网络抖动断线 | 程序会先等待 KoKo 原生重连，超过其重试窗口后自动刷新 Luna；若登录已过期，会提示重新登录 |
| 弹出"需要登录"提示 | 堡垒机会话过期，在弹出的浏览器窗口重新登录一次即可 |
| 目标机会话建立很慢 | 脚本最多等 3 分钟，期间每 30 秒打印一次进度；目标机负载高时属正常现象 |
| 输出为空 | 查看 `luna_term.log`（大文件传输内容除外）确认会话是否正常 |
| 命令未执行 | 确认终端已就绪（出现 shell 提示符）；复杂命令先在小范围测试 |
| WebSFTP 上传失败 | 默认会自动回退；也可用 `--upload-method terminal` 强制兼容通道 |
| WebSFTP 下载失败 | 默认会自动回退；也可用 `--download-method terminal` 强制兼容通道 |
| 目标目录不存在 | 先在远端创建目录；上传不会隐式创建目标目录 |
| 堡垒机会话审计 | **每次运行都会在 JumpServer 留下一条会话记录**，按需运行，注意合规 |

---

## 六、更换目标资产

修改本地 `luna_config.json`，无需修改源代码。`asset_id` 和 `org_id` 可从
资产连接页 URL 的 `login_to`、`oid` 参数取得；`sftp_nav` 是文件管理器中
从根目录到目标资产的逐级名称。

---

## 七、文件清单

| 文件 | 说明 |
|---|---|
| `luna_cli.py` | 主程序（唯一代码文件） |
| `run.bat` | Windows 快捷启动脚本（可选） |
| `requirements.txt` | Python 依赖 |
| `luna_config.example.json` | 不含敏感值的配置模板 |
| `luna_config.json` | 本地目标配置（敏感，已被 git 忽略） |
| `storage_state.json` | 登录会话（敏感，勿外传/提交到 git） |
| `luna_term.log` | 终端输出日志（运行时自动追加） |
| `last_output.txt` | 最近一次 `-c` 命令的完整输出（运行时生成） |

`limit_test_1g.bin`、`speed_test_*.bin` 等测速文件不属于运行依赖，完成测速后
可以删除。`storage_state.json` 包含登录令牌，应限制访问并避免复制或提交。
