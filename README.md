# CMO Agent Bridge

让 Codex、Claude Code、Cursor 等 Agent 直接读取和操作本机运行中的
**Command: Modern Operations（CMO）**想定。

[![CI](https://github.com/Nuclear2/cmo-agent-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/Nuclear2/cmo-agent-bridge/actions/workflows/ci.yml)
[![Platform](https://img.shields.io/badge/platform-Windows-0078D6.svg)](docs/installation.md)
[![Release](https://img.shields.io/github/v/release/Nuclear2/cmo-agent-bridge?include_prereleases&label=release)](https://github.com/Nuclear2/cmo-agent-bridge/releases)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **平台支持：仅限 Windows。**

[下载 v0.1.4](https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.1.4) ·
[快速上手](docs/quickstart.md) ·
[各框架安装](docs/frameworks/README.md) ·
[CMO Lua API](https://commandlua.github.io/)

CMO Agent Bridge 将推演和想定制作中常用的 Lua API 整理成 70 个结构化 MCP 工具。Agent
可以读取战场态势、创建任务、分配兵力、规划航路和加油，也可以协助制作单位、天气与事件。
所有通信都留在本机，CMO 侧只需运行一个很短的轮询事件。

> **当前版本是 v0.1.4 预览版。** 已在 Windows、CMO Build 1868 上验证；第一次接入建议使用
> 想定副本。

## 你可以直接这样说

> 以 `LIVE_PLAYER` 模式连接当前想定。先汇总我方空中态势和已知威胁，再给出 CAP 调整建议，
> 暂时不要修改想定。

> 在现有两个 CAP 区中间创建一个新任务区，把所有 J-36 分配过去；完成后读回任务和分配结果。

> 以 `SCENARIO_AUTHOR` 模式，为这个想定添加一个进入区域后触发的增援事件，并检查触发器、
> 条件和动作是否关联正确。

它适合三类工作：

| 场景 | 可以完成的工作 |
|---|---|
| 推演 | 态势评估、任务规划、兵力分配、条令与 WRA、EMCON、航路、加油、交战和时间倍率 |
| 想定制作 | 阵营、单位、任务、天气、时间线、事件、Special Action、计分和部分库存设置 |
| 测试与裁决 | 受控注入、想定检查、故障诊断和裁决记录 |

## 快速开始（Windows）

下面以 Steam 默认安装路径和 PowerShell 为例。完整的升级、卸载和自定义路径说明见
[安装文档](docs/installation.md)。

### 1. 安装 uv

bridge 由 [`uv`](https://docs.astral.sh/uv/getting-started/installation/) 管理，在 PowerShell 中执行：

```powershell
winget install --id astral-sh.uv -e
uv --version
```

需要 `uv 0.11.26` 或更高版本；如果已安装但版本较旧，运行
`winget upgrade --id astral-sh.uv -e`。`uvx` 会为 bridge 准备独立的 Python 3.12 环境和运行依赖。

### 2. 安装 Agent 插件

#### 只有 ChatGPT / Codex Desktop

如果没有可用的 `codex` CLI，下载并运行 Desktop 安装脚本：

```powershell
$installer = Join-Path $env:TEMP "install-codex-desktop.ps1"
Invoke-WebRequest `
  -UseBasicParsing `
  -Uri "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.4/install-codex-desktop.ps1" `
  -OutFile $installer
powershell.exe -NoProfile -ExecutionPolicy Bypass -File $installer
```

脚本会把本地插件源加入 **Plugins → Personal**，不需要 Codex CLI。完全退出并重启 Desktop，
然后在 **Personal** 中打开 `cmo-agent-bridge` 并点击安装。插件运行 MCP 时仍需要第 1 步安装的
`uv` / `uvx`。

#### 有 Codex CLI

```powershell
codex plugin marketplace add Nuclear2/cmo-agent-bridge --ref stable
codex plugin add cmo-agent-bridge@cmo-tools
```

第二条命令也可以改为在 Codex 中打开 `/plugins`，从 **CMO Tools** 安装
`cmo-agent-bridge`。远程自建 marketplace 不会自动出现在官方插件目录中，必须先执行第一条命令。
`stable` 只会在 Release 完整发布后推进；Codex 启动时会自动检查更新，也可以运行
`codex plugin marketplace upgrade cmo-tools` 立即刷新。

#### Claude Code

```powershell
claude plugin marketplace add Nuclear2/cmo-agent-bridge@stable
claude plugin install cmo-agent-bridge@cmo-tools --scope user
```

Claude Code 默认关闭第三方 marketplace 的自动更新。安装后可在 `/plugin` → **Marketplaces** →
`cmo-tools` 中启用 auto-update；也可以手动运行
`claude plugin update cmo-agent-bridge@cmo-tools --scope user`。执行 `/reload-plugins` 或新建会话
即可载入。Codex 和 Claude 的 plugin 都同时包含 MCP 配置和完整的 `operate-cmo` Skill。

OpenCode、Cursor、Qoder 和通用 MCP 客户端的配置见[各框架安装](docs/frameworks/README.md)。这些
框架必须同时注册本地 `stdio` MCP Server，并安装完整的 `operate-cmo` Skill；MCP 协议本身不会
携带 Skill。

### 3. 部署 CMO 侧运行时

重启 Agent 并新建任务，然后直接告诉它：

> 调用 `cmo_bridge_diagnose` 检查当前安装；如果还没准备好，用
> `D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations` 作为
> `game_root` 调用 `cmo_bridge_prepare`。

`cmo_bridge_prepare` 会部署与插件版本匹配的 Lua runtime，并让当前 MCP 会话里的普通工具立即
可用，不需要再次重启。CMO 安装在其他位置时，把提示中的路径换成实际路径即可。CLI 只作为
[安装与故障排查](docs/installation.md)时的备用入口。

插件仍固定使用同版本 wheel：

```powershell
$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.4/cmo_agent_bridge-0.1.4-py3-none-any.whl"
```

### 4. 在想定中保存轮询事件

这一步由想定作者做一次。打开想定编辑器，新建一个启用且可重复执行的 event：

1. 添加 **Regular Time** trigger，间隔设为 1 秒；
2. 添加 **Lua Script** action，内容为：

```lua
return ScenEdit_RunScript('CMOAgentBridge/inbox/request.lua')
```

3. 把 trigger 和 action 关联到该 event，然后保存想定。

事件随想定保存后，普通推演模式也可以直接使用 bridge。Regular Time 会在想定时间流动时处理请求；
需要 Agent 操作时将时间倍率调到 1x，操作完成后再恢复常用倍率。

### 5. 确认连接

保持 CMO 和目标想定运行，告诉 Agent：

> 调用 `cmo_bridge_status`，告诉我当前 CMO build、runtime tag 和想定 lineage。

返回成功结果，说明 CMO 侧已经接通。如果请求一直等待，检查轮询 event 是否启用、是否允许重复，
以及想定时间是否正在推进。如果 Agent 中没有出现 `cmo_*` 工具，检查插件与 `uvx` 后重启 Agent
并新建任务。

## 推演、想定制作与测试

Skill 会根据工作内容采用不同的信息范围：

| 模式 | 信息范围 | 适用工作 |
|---|---|---|
| `LIVE_PLAYER` | 己方状态和己方观察到的 contacts | 正常推演、部署、交战与保障 |
| `SCENARIO_AUTHOR` | 完整想定状态 | 制作或修改想定、事件、兵力与计分 |
| `UMPIRE` | 获准裁决范围内的完整状态 | 测试、注入、诊断和裁决 |

日常推演使用 `LIVE_PLAYER`；制作想定或进行测试裁决时，再切换到相应模式。这样既能让 Agent
充分利用编辑器能力，也能保留正常推演中的情报不确定性。

## 工作原理

```text
Agent  ── stdio / MCP ──>  cmo-bridge  ── 本机文件桥 ──>  CMO Lua 事件
```

- **MCP Server** 提供 70 个 `cmo_*` 工具，负责主机准备、诊断以及读取和修改 CMO 状态；
- **operate-cmo Skill** 提供态势评估、作战规划、执行检查和想定制作流程；
- **CLI** 用于安装运行时、诊断连接和人工测试；
- **Plugin** 为 Codex 和 Claude Code 打包 MCP 配置与 Skill，其他框架可以直接注册标准
  `stdio` MCP Server。

bridge 以本地 `stdio` 进程运行。Agent、Python 进程和 CMO Lua 运行时通过本机文件交换请求与
结果，状态记录保存在本地 SQLite 中。

## 文档

- [快速上手](docs/quickstart.md)
- [安装、升级与卸载](docs/installation.md)
- [各 Agent 框架配置](docs/frameworks/README.md)
- [变更记录](CHANGELOG.md)
- [安全说明](SECURITY.md)
- [参与贡献](CONTRIBUTING.md)
- [CMO 官方 Lua API](https://commandlua.github.io/)

## 项目状态

- 当前版本：[`v0.1.4 Preview`](https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.1.4)
- 已验证环境：Windows 10/11、CMO Build 1868
- Python：3.12，由 `uv` 隔离管理
- 许可证：[MIT](LICENSE)

这是一个非官方社区项目，与 WarfareSims、Matrix Games、Slitherine 或 CMO 的开发商、发行商
没有隶属关系。Agent 的操作会直接修改当前想定，首次使用前请保存副本；脚本执行等详细注意事项见
[安全说明](SECURITY.md)。
插件和 Skill 中的 CMO 图标取自游戏随附的 `Command.ico`；相关权利归原权利人所有，不受本项目
MIT 许可证覆盖。
