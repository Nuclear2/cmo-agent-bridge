# Cursor

Cursor 从全局 `~/.cursor/mcp.json` 或项目级 `.cursor/mcp.json` 加载 MCP server。

## MCP 配置

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

最新 Cursor 文档要求本地 server 明确使用 `"type": "stdio"`。如果桌面应用无法继承 PATH，
把 `command` 改为 `Get-Command cmo-bridge` 返回的完整路径，并把 `\` 转义成 `\\`。

保存后完全重启 Cursor，或在 **Customize > MCP** 中重新加载。项目配置适合只在某一项目启用，
全局配置适合在任何工作区指挥本机 CMO。

## Skill

Cursor 自动发现 Agent Skills。把完整目录复制到：

```text
~/.cursor/skills/operate-cmo/
```

也可使用通用目录：

```text
~/.agents/skills/operate-cmo/
```

项目级对应 `.cursor/skills/operate-cmo/` 或 `.agents/skills/operate-cmo/`。只安装 skill 不会创建
MCP 连接。

## 验证

在 **Customize > MCP** 中确认 `cmo` 已连接，然后新建 Agent 对话并要求调用
`cmo_bridge_diagnose`，需要时调用 `cmo_bridge_prepare`，再调用 `cmo_bridge_status`。Cursor 默认
可能在首次工具调用时请求批准。

官方参考：[Cursor MCP](https://cursor.com/docs/mcp.md)、
[Cursor Skills](https://cursor.com/docs/skills.md)。
