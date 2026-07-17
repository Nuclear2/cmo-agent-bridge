# CMO Agent Bridge

让 Codex、Claude Code、OpenCode、Cursor、Qoder 以及其他 MCP 客户端，直接操作本机正在运行的
**Command: Modern Operations (CMO)**。

项目提供一个本地 `stdio` MCP server、68 个有类型的 CMO 工具、一套用于战役推演与想定制作的
Agent Skill，以及用于安装、诊断和人工调用的 CLI。它不启动网络服务：主机程序与 CMO 内的 Lua
运行时通过本机文件桥通信。

> 当前预览版：[`v0.1.0 Preview`](https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.1.0)
> 已验证环境：Windows、CMO Build 1868、Python 3.12（由 `uv` 隔离管理）

`v0.1.0` 会以 GitHub **Pre-release** 发布，面向愿意保存备份、反馈兼容性问题的早期用户；它不是
稳定正式版。Agent 的写操作会真实改变想定，首次使用请从副本开始。

## 能做什么

- 读取战场态势：想定、阵营、单位、接触、任务、条令、WRA、EMCON、传感器、武器与后勤状态。
- 执行正常玩家命令：创建和配置任务、分配单位、规划任务区与飞行计划、设置加油、发射、返航、
  攻击、航路和时间倍率等。
- 协助想定作者：创建阵营和单位，编辑天气、时间线、事件、触发器/条件/动作、Special Action、
  计分与部分库存状态。
- 用 skill 约束情报边界和工作流程，区分公平推演与全知编辑，避免把想定作者权限混入玩家决策。

## 开始前

你需要：

- 已安装并能正常运行的 CMO；
- Windows 10/11；
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/)；
- 一个已经挂载桥接轮询事件的想定。普通玩家模式不能临时编辑事件，因此公开或分发想定前，
  想定作者必须先把事件保存进想定文件。

CMO、Matrix Games 与 Lua API 不随本项目分发。

## 三步安装

### 1. 安装 bridge

在 PowerShell 中执行：

```powershell
winget install --id astral-sh.uv -e

$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.0/cmo_agent_bridge-0.1.0-py3-none-any.whl"
uv tool install --python 3.12 $wheel
uv tool update-shell
```

重新打开终端和 Agent 应用后确认：

```powershell
Get-Command cmo-bridge
cmo-bridge --help
```

### 2. 部署 Lua 运行时并挂载事件

```powershell
cmo-bridge prepare `
  --game-root "D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations"
```

在想定编辑器中创建并保存一个启用、可重复的事件：

1. 添加间隔为 1 秒的 **Regular Time** trigger；
2. 添加 **Lua Script** action，内容为：

```lua
return ScenEdit_RunScript('CMOAgentBridge/inbox/request.lua')
```

3. 把 trigger 和 action 关联到同一个启用且可重复执行的 event。

Regular Time 只在想定时间流动时触发。复杂操作时让游戏保持 1x；完成并校验后可恢复更高倍率。
如果游戏被暂停，先恢复到 1x 再发出 bridge 请求。

### 3. 连接你的 Agent

通用安装后的启动命令是：

```text
cmo-bridge serve
```

请选择你的框架：

- [Codex](docs/frameworks/codex.md)
- [Claude Code](docs/frameworks/claude-code.md)
- [OpenCode](docs/frameworks/opencode.md)
- [Cursor](docs/frameworks/cursor.md)
- [Qoder](docs/frameworks/qoder.md)
- [其他 MCP 客户端](docs/frameworks/generic-mcp.md)

完整安装、升级和卸载说明见 [docs/installation.md](docs/installation.md)。

## CMO 模式与 Agent 模式不是一回事

CMO 的“想定编辑器/普通推演模式”决定 Lua 事件能否被创建或修改；skill 的工作模式决定 Agent
可以使用哪些信息和权限：

| Agent 模式 | 信息边界 | 用途 |
|---|---|---|
| `LIVE_PLAYER` | 只使用己方数据；敌方仅通过己方接触信息判断 | 公平推演、部署、交战与保障 |
| `SCENARIO_AUTHOR` | 按需使用全知想定状态 | 制作或修改想定、事件、兵力与计分 |
| `UMPIRE` | 仅在获准的裁决范围内使用全知状态 | 测试、注入、诊断和裁决 |

每个工作流默认从 `LIVE_PLAYER` 开始。只有用户明确要求制作想定、全知检查或裁决时，Agent 才应
切换到 `SCENARIO_AUTHOR` 或 `UMPIRE`。

## MCP、Skill、Plugin 和 CLI 的边界

- **MCP server** 是真正执行 CMO 操作的跨框架标准接口；68 个 `cmo_*` tools 都来自这里。
- **Skill** 只提供态势评估、作战规划、想定制作和工具选择规范；它不会自行连接或修改 CMO。
- **Plugin** 是面向特定 Agent 框架的发行包装，可携带 MCP 启动配置与 skill。本项目的 plugin
  不内嵌 wheel；其 MCP 配置通过固定版本的 `uvx` 启动 GitHub Release wheel。
- **CLI** 主要给安装、诊断、自动化和人工冒烟测试使用。Agent 正常工作时应优先调用 MCP tools，
  不应反复生成 shell 命令代替结构化工具调用。

只安装 MCP 也能操作 CMO，但 Agent 不会自动获得 skill 中的作战流程与权限约束。跨框架安装建议
同时完成 MCP 注册和 skill 安装；Codex、Claude Code 可直接使用仓库提供的 marketplace plugin。

## 第一次验证

保持 CMO、目标想定和轮询事件运行，在 PowerShell 中执行：

```powershell
cmo-bridge invoke bridge.status --args '{}'
cmo-bridge invoke scenario.get --args '{}'
```

成功结果包含 `"ok": true`。随后新建一个 Agent 会话，要求它调用 `cmo_bridge_status`；如果工具
列表没有刷新，请完全重启该 Agent 应用或新建会话。

## 文档

- [快速上手](docs/quickstart.md)
- [安装、升级与卸载](docs/installation.md)
- [各 Agent 框架配置](docs/frameworks/README.md)
- [变更记录](CHANGELOG.md)
- [安全说明](SECURITY.md)
- [参与贡献](CONTRIBUTING.md)
- [CMO 官方 Lua API](https://commandlua.github.io/)

## 免责声明

本项目是非官方社区工具，与 WarfareSims、Matrix Games、Slitherine 或 Command: Modern Operations
的开发商/发行商无隶属或背书关系。CMO 名称、游戏文件及相关商标归其权利人所有。

Agent 的写操作会真实改变当前想定。首次使用、想定制作和破坏性操作前请保存副本；不要把未经
审查的 MCP server、skill 或 Lua 脚本接入重要想定。

bridge 没有单调用的通用 `lua.eval`/`lua.call` 工具，但想定作者可以通过 LuaScript event component
或 Special Action 保存 Lua，并在激活/执行后让它运行于本机 CMO 进程。这个组合等价于本机代码
执行，只允许在可信的 `SCENARIO_AUTHOR`/`UMPIRE` 范围使用：逐行审查完整脚本、保存想定副本、
默认保持 inactive 且不可重复、读回核对后再启用。`LIVE_PLAYER` 不得创建、修改或测试这类脚本。
