# Codex

Codex plugin 同时安装 CMO 的 MCP 配置和完整 `operate-cmo` Skill。先安装
[`uv`](https://docs.astral.sh/uv/getting-started/installation/)；每个 plugin Release 都会通过
`uvx` 启动同版本的 bridge。

## 只有 ChatGPT / Codex Desktop

如果没有可用的 `codex` CLI，下载 v0.2.1 Release 中的本地安装脚本，再从磁盘执行：

```powershell
$installer = Join-Path $env:TEMP "install-codex-desktop.ps1"
Invoke-WebRequest `
  -UseBasicParsing `
  -Uri "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.2.1/install-codex-desktop.ps1" `
  -OutFile $installer
powershell.exe -NoProfile -ExecutionPolicy Bypass -File $installer
```

不建议用 `irm | iex` 直接执行网络内容。该脚本只把 marketplace 和 plugin 安装到 Codex 的本地
Personal 插件目录，不安装 Codex CLI，也不编辑 `config.toml` 或插件缓存。

脚本结束后：

1. 完全退出并重启 ChatGPT / Codex Desktop；
2. 打开 **Plugins → Personal**；
3. 打开 `cmo-agent-bridge` 并点击安装；
4. 新建任务，使 MCP tools 和 Skill 进入新会话。

这条路径仍需要 `uv` / `uvx`，但不需要 `codex` 命令。

## 有 Codex CLI

先注册只跟随完整 Release 的 `stable` marketplace，再安装 plugin：

```powershell
codex plugin marketplace add Nuclear2/cmo-agent-bridge --ref stable
codex plugin add cmo-agent-bridge@cmo-tools
```

也可以只执行第一条命令，然后在 Codex 中打开 `/plugins`，从 **CMO Tools (`cmo-tools`)** 安装
`cmo-agent-bridge`。GitHub 上的远程自建 marketplace 不会自动出现在 Codex 官方插件目录中；
必须先注册，才能通过命令或 `/plugins` 找到它。

Codex 启动时会自动刷新 Git marketplace。要立即检查更新，运行
`codex plugin marketplace upgrade cmo-tools`；更新后新建任务，必要时完整重启 Desktop。

安装完成后重启 Codex 并新建任务。如果之前手工注册过相同的 CMO MCP server，请删除或禁用其中
一个，避免同名工具重复。

## 只注册 MCP

不使用 plugin 时，可以只注册 stdio server：

```powershell
$bridge = (Get-Command cmo-bridge).Source
codex mcp add cmo -- $bridge serve
codex mcp list
```

也可以编辑用户级 `~/.codex/config.toml`：

```toml
[mcp_servers.cmo]
command = "cmo-bridge"
args = ["serve"]
startup_timeout_sec = 30
tool_timeout_sec = 120
enabled = true
```

项目级配置可放在受信任项目的 `.codex/config.toml`。若使用绝对 Windows 路径，TOML 可用单引号
避免转义，例如：

```toml
command = 'C:\Users\you\.local\bin\cmo-bridge.exe'
```

仅注册 MCP 不会安装 Skill。还需从 `v0.2.1` 标签复制完整目录
`plugins/cmo-agent-bridge/skills/operate-cmo` 到 `~/.agents/skills/operate-cmo/`；项目级可使用
`<project>/.agents/skills/operate-cmo/`。必须包含 `SKILL.md`、`agents/` 和 `references/`。

## 完成 CMO 侧设置

无论选择哪条 Codex 安装路径，都要让 Agent 调用 `cmo_bridge_diagnose` 和所需的
`cmo_bridge_prepare`，并在想定中保存轮询事件。安装 plugin 不会自动修改 CMO 想定。

## 验证

在 Codex 中新建任务并要求：

```text
先调用 cmo_bridge_diagnose；需要时在当前任务调用 cmo_bridge_prepare，然后调用
cmo_bridge_status，确认当前 CMO build、runtime tag 和想定 lineage。
```

如果没有看到工具，确认 `uvx` 可用、plugin 已在 UI 中安装，并完全重启 Codex 后新建任务。CLI
用户还可以运行 `codex mcp list` 检查 server 状态。

官方参考：[Codex MCP](https://learn.chatgpt.com/docs/extend/mcp)、
[Build plugins](https://learn.chatgpt.com/docs/build-plugins)、
[Build skills](https://learn.chatgpt.com/docs/build-skills)。
