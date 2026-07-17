# Qoder

Qoder CLI 可以直接添加本地 `stdio` MCP server，并在新会话中自动启动它。

## MCP 配置

用户级安装：

```powershell
$bridge = (Get-Command cmo-bridge).Source
qodercli mcp add -s user cmo -- $bridge serve
qodercli mcp list
```

默认 scope 是当前项目的 local 配置；`-s user` 让它对当前用户的所有项目可用。团队希望提交项目级
配置时，可在项目根目录使用标准 `.mcp.json`：

```json
{
  "mcpServers": {
    "cmo": {
      "type": "stdio",
      "command": "cmo-bridge",
      "args": ["serve"]
    }
  }
}
```

Qoder 的用户配置位于 `~/.qoder/settings.json`，local 项目配置位于
`.qoder/settings.local.json`，project scope 使用 `.mcp.json`。

## Skill

把完整 `plugins/cmo-agent-bridge/skills/operate-cmo` 目录复制到：

```text
~/.qoder/skills/operate-cmo/
```

项目级使用 `<project>/.qoder/skills/operate-cmo/`。MCP 和 skill 是两项独立安装。

## 验证

如果 Qoder CLI 已经在运行，执行 `/mcp reload`；否则新建会话。运行 `qodercli mcp list` 并要求
Agent 调用 `cmo_bridge_diagnose`，需要时调用 `cmo_bridge_prepare`，再调用 `cmo_bridge_status`。
默认权限模式可能要求确认第一次 MCP tool call。

官方参考：[Qoder MCP Servers](https://docs.qoder.com/en/cli/mcp-servers)、
[Qoder Skills](https://docs.qoder.com/en/cli/Skills)。
