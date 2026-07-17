# 安装、升级与卸载

> `v0.1.2` 是 **Preview / GitHub Pre-release**，不是稳定正式版。请先备份想定，并优先在测试副本
> 上验证当前 CMO build、任务流程和写操作。

## 推荐方案：安装 Release wheel

### 前提

- Windows 10/11；
- Command: Modern Operations；
- PowerShell 7 或 Windows PowerShell 5.1；
- 可用的 Agent 框架或其他本地 MCP 客户端。

### 安装 uv

需要 `uv 0.11.26` 或更高版本。如果 `uv --version` 显示的版本已满足要求，可跳过本节。

```powershell
winget install --id astral-sh.uv -e
uv --version
```

已安装但版本较旧时，执行：

```powershell
winget upgrade --id astral-sh.uv -e
uv --version
```

若 `uv` 安装后当前终端仍找不到它，关闭并重新打开 PowerShell。其他官方安装方式见
[uv installation](https://docs.astral.sh/uv/getting-started/installation/)。

### 安装 v0.1.2 Preview

```powershell
$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.2/cmo_agent_bridge-0.1.2-py3-none-any.whl"
uv tool install --python 3.12 $wheel
uv tool update-shell
```

`uv tool install` 会为 bridge 创建独立环境，不修改全局 Python，也不要求系统已经安装 Python
3.12。重新打开终端后检查：

```powershell
Get-Command cmo-bridge
cmo-bridge --help
```

如果 Agent 是桌面应用，也要完全退出并重开，使它继承更新后的 `PATH`。若仍找不到命令，取得
可执行文件的绝对路径并在 MCP 配置中使用该路径：

```powershell
(Get-Command cmo-bridge).Source
```

JSON 配置中的 Windows 反斜杠要写成 `\\`。

## 部署 CMO 侧运行时

使用 plugin 时，推荐新建 Agent 任务并让它先调用 `cmo_bridge_diagnose`；若结果不是 `ready`，
再由它调用 `cmo_bridge_prepare`。准备成功后，同一 MCP 会话立即可用。下面的 CLI 命令用于手工
安装、自动化或 MCP 工具无法加载时的备用路径。

```powershell
cmo-bridge prepare `
  --game-root "D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations"
```

如 CMO 安装在其他位置，请替换路径。命令会：

- 把版本绑定的 Lua dispatcher 部署到 `Lua\CMOAgentBridge\versions\...`；
- 创建 `Lua\CMOAgentBridge\inbox\request.lua` 和 `Lua\CMOAgentBridge\poll.lua`；
- 把 game root 保存到 `%LOCALAPPDATA%\CMOAgentBridge\config.toml`；
- 在 JSON 输出的 `lua_action` 字段中给出要挂载的脚本。

如果之前保存的是另一个 CMO 安装目录，明确替换它：

```powershell
cmo-bridge prepare `
  --game-root "D:\Games\Command - Modern Operations" `
  --replace-saved-game-root
```

每次升级 bridge 后都要重新运行 `prepare`，确保主机 wheel 和 CMO 侧 Lua runtime 属于同一发布版。

## 在想定中挂载轮询事件

在 CMO 想定编辑器里创建：

1. 一个间隔为 1 秒的 **Regular Time** trigger；
2. 一个 **Lua Script** action：

```lua
return ScenEdit_RunScript('CMOAgentBridge/inbox/request.lua')
```

3. 一个已启用且可重复执行的 event，并把上述 trigger 和 action 关联到它；
4. 保存想定。

### 普通玩家模式的重要限制

普通推演模式不会提供想定编辑器。因此：

- 如果想定文件已经保存了桥接事件，bridge 在普通玩家模式下可以正常工作；
- 如果想定没有该事件，玩家无法仅靠 MCP 或 CLI 临时补挂；
- 想定作者应在发布前预置并测试事件；旧想定则需先用编辑器修改并另存。

Regular Time trigger 只在想定时间推进时执行。游戏暂停时请求不会被处理；恢复到 1x 后，下一次
轮询会处理待办请求。复杂的多步规划应先降到 1x，刷新态势、执行并读回校验，再恢复原倍率。

## 安装 Agent 集成

### 只有 ChatGPT / Codex Desktop

没有可用的 `codex` CLI 时，下载 Release 脚本后再执行：

```powershell
$installer = Join-Path $env:TEMP "install-codex-desktop.ps1"
Invoke-WebRequest `
  -UseBasicParsing `
  -Uri "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.2/install-codex-desktop.ps1" `
  -OutFile $installer
powershell.exe -NoProfile -ExecutionPolicy Bypass -File $installer
```

不要用 `irm | iex` 直接执行网络内容。脚本只安装本地 Personal marketplace 与 plugin 源，不会安装
Codex CLI，也不会编辑 `config.toml` 或插件缓存。完全退出并重启 Desktop 后，在
**Plugins → Personal** 中打开 `cmo-agent-bridge` 并点击安装。此路径仍需要 `uv` / `uvx`。

### 有 Codex CLI

```powershell
codex plugin marketplace add Nuclear2/cmo-agent-bridge --ref v0.1.2
codex plugin add cmo-agent-bridge@cmo-tools
```

也可以注册 marketplace 后打开 `/plugins`，从 **CMO Tools** 安装。远程自建 marketplace 不会
自动出现在官方插件目录中，必须先完成第一条命令。

### Claude Code

```powershell
claude plugin marketplace add Nuclear2/cmo-agent-bridge@v0.1.2
claude plugin install cmo-agent-bridge@cmo-tools --scope user
```

Codex 和 Claude plugin 都同时安装 MCP 配置与完整 `operate-cmo` Skill。

### OpenCode、Cursor、Qoder 与其他 MCP 客户端

这些框架需要完成两件事：注册下面的 stdio MCP server，并按“安装 operate-cmo skill”一节复制
完整 Skill。MCP 只提供工具，不会携带 Skill。

## 注册 MCP server

安装后的标准 `stdio` 启动命令是：

```powershell
cmo-bridge serve
```

不要在普通终端里等待它输出；它会等待 MCP 客户端通过 stdin 发送协议帧。按照所用框架配置：

- [Codex](frameworks/codex.md)
- [Claude Code](frameworks/claude-code.md)
- [OpenCode](frameworks/opencode.md)
- [Cursor](frameworks/cursor.md)
- [Qoder](frameworks/qoder.md)
- [通用 MCP 客户端](frameworks/generic-mcp.md)

plugin 不在 zip 中重复内嵌 wheel；它用固定版本的 `uvx` URL 启动同一个 Release wheel。持久安装
wheel 仍是运行 `prepare`、CLI 诊断和手工测试最直接的方式。

## 安装 operate-cmo skill

MCP server 提供工具，skill 提供如何评估、规划和安全使用这些工具的知识。若框架不使用本仓库的
plugin marketplace，可下载 Release 中的独立 Skill 包：

```powershell
$skillZip = Join-Path $env:TEMP "operate-cmo-skill-0.1.2.zip"
$skillRoot = Join-Path $env:TEMP "operate-cmo-skill-0.1.2"
Invoke-WebRequest `
  -UseBasicParsing `
  -Uri "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.2/operate-cmo-skill-0.1.2.zip" `
  -OutFile $skillZip
Expand-Archive -LiteralPath $skillZip -DestinationPath $skillRoot -Force
```

解压后的完整 Skill 目录是 `$skillRoot\operate-cmo\`，其中包含 `SKILL.md`、`agents/` 和
`references/`。把整个 `operate-cmo` 目录复制到对应 Agent 框架的 Skill 搜索路径。

如果机器上已安装 Git，也可从标签固定的源码取得：

```powershell
git clone --depth 1 --branch v0.1.2 https://github.com/Nuclear2/cmo-agent-bridge.git
cd cmo-agent-bridge
```

要复制的是：

```text
plugins/cmo-agent-bridge/skills/operate-cmo/
```

必须复制整个目录，不能只复制入口文件。各框架的目标目录见对应配置文档。

## 免安装运行：uvx

不想持久安装 CLI 时，可让 `uvx` 从 Release wheel 启动隔离环境：

```powershell
$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.2/cmo_agent_bridge-0.1.2-py3-none-any.whl"
uvx --python 3.12 --from $wheel cmo-bridge --help
uvx --python 3.12 --from $wheel cmo-bridge prepare `
  --game-root "D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations"
```

对应的 MCP 命令为：

```text
uvx --python 3.12 --from https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.2/cmo_agent_bridge-0.1.2-py3-none-any.whl cmo-bridge serve
```

`uvx` 不创建持久 tool 安装，但会使用 `uv` 的下载缓存。它适合试用和固定版本的可移植配置；长期
使用推荐 `uv tool install`，启动配置更短，也更容易排查 PATH 和版本问题。

## 从源码开发

```powershell
git clone https://github.com/Nuclear2/cmo-agent-bridge.git
cd cmo-agent-bridge
uv sync --locked
uv run --locked cmo-bridge --help
```

源码开发运行不等同于最终用户安装；MCP 客户端若要从源码启动，必须把 `cwd` 指向这个正常克隆
的仓库根目录，并使用 `uv run --locked cmo-bridge serve`。

## 冒烟测试

保持 CMO、目标想定和轮询事件运行，且时间至少为 1x：

```powershell
cmo-bridge invoke bridge.status --args '{}'
cmo-bridge invoke scenario.get --args '{}'
```

成功时输出包含 `"ok": true`。若超时，依次检查：

1. `Command.exe` 是否正在运行；
2. CMO 是否打开了正确想定；
3. event 是否启用、可重复并关联了正确的 trigger/action；
4. 想定时间是否正在推进；
5. `prepare` 是否在当前 bridge 版本安装后重新执行；
6. Agent 是否启动了一个新的会话并加载了 MCP server。

CLI 适合诊断；Agent 的正常操作应通过 `cmo_*` MCP tools 完成。

## 升级

查看目标 Release 的 wheel 文件名，然后执行：

```powershell
$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.2/cmo_agent_bridge-0.1.2-py3-none-any.whl"
uv tool install --force --python 3.12 $wheel
cmo-bridge prepare `
  --game-root "D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations"
```

随后更新 plugin/skill，重启 Agent 并新建会话。不要让旧 MCP 进程和新 Lua runtime 混用。

## 卸载

先从 Agent 框架中移除 `cmo` MCP server 或 marketplace plugin，然后：

```powershell
uv tool uninstall cmo-agent-bridge
```

bridge 的本机配置和状态位于 `%LOCALAPPDATA%\CMOAgentBridge`，CMO 侧文件位于
`<game-root>\Lua\CMOAgentBridge`。只有确认不再有想定或 Agent 使用它们后，才手工删除这些目录。
想定内的 event 属于想定文件本身，需要由想定作者在编辑器中移除并重新保存。
