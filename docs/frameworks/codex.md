# Codex

Codex 支持本地 `stdio` MCP。推荐使用 marketplace plugin，因为它同时携带 bridge MCP 配置和
`operate-cmo` skill。

## 方案 A：从 marketplace 安装 plugin（Desktop / CLI）

先把固定到预览版标签的 GitHub marketplace 加入 Codex：

```powershell
codex plugin marketplace add Nuclear2/cmo-agent-bridge --ref v0.1.0
```

公开文档没有提供面向最终用户的直接 plugin install/add 命令。添加 marketplace 后请选择一种受支持
的安装界面：

- **Codex CLI**：运行 `codex`，输入 `/plugins`，切换到 **CMO Tools (`cmo-tools`)** marketplace，
  打开 `cmo-agent-bridge` 并选择安装/启用；
- **ChatGPT Desktop**：选择 Codex，打开 **Plugins**，在 CMO Tools 来源中打开
  `cmo-agent-bridge`，点击安装。

安装后新建任务。plugin 会注册 MCP server 并提供 skill；仍需先按照
[安装文档](../installation.md)运行 `cmo-bridge prepare`，并在想定里挂载轮询事件。plugin 不内嵌
wheel，它通过固定版本的 `uvx` Release URL 启动 MCP server。

**Codex IDE extension 不支持 plugin 浏览或安装。** IDE 用户请使用下面的方案 B 注册 MCP，再按
“只安装 skill”一节复制 skill。Codex CLI、Desktop 和 IDE 在同一 host 上共享 Codex MCP 配置。

如果已经手工添加了名为 `cmo` 的 MCP server，请删除或禁用其中一个，避免重复工具。

## 方案 B：只注册 MCP

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

## 只安装 skill

从标签 `v0.1.0` 克隆源码后，把整个目录
`plugins/cmo-agent-bridge/skills/operate-cmo` 复制到：

```text
~/.agents/skills/operate-cmo/
```

项目级可使用 `<project>/.agents/skills/operate-cmo/`。skill 不会启动 MCP；必须同时完成方案 A
或方案 B。IDE extension 应使用方案 B。

## 验证

在 Codex 中新建任务并要求：

```text
调用 cmo_bridge_status，确认当前 CMO build、runtime tag 和想定 lineage。
```

若看不到工具，运行 `codex mcp list`，确认 server 已启用，然后完全重启 Codex 并新建任务。

官方参考：[Codex MCP](https://developers.openai.com/codex/mcp/)、
[Codex Plugins](https://developers.openai.com/codex/plugins/)、
[Codex Skills](https://developers.openai.com/codex/skills/)。
