# Claude Code

Claude Code 支持本地 `stdio` MCP、Agent Skills 和 Git marketplace plugin。

## 方案 A：安装 plugin（推荐）

```powershell
claude plugin marketplace add Nuclear2/cmo-agent-bridge@v0.1.2
claude plugin install cmo-agent-bridge@cmo-tools --scope user
```

在已打开的 Claude Code 会话中执行 `/reload-plugins`，或新建会话。plugin 同时携带 MCP 配置和
完整的 `operate-cmo` Skill（包括引用文档）；Agent 可以在 MCP 内运行 `cmo_bridge_prepare`，目标
想定仍需保存轮询事件。

## 方案 B：只注册 MCP

注册到用户范围：

```powershell
$bridge = (Get-Command cmo-bridge).Source
claude mcp add --transport stdio --scope user cmo -- $bridge serve
claude mcp list
```

若希望随项目共享，可在项目根目录使用 `--scope project`，Claude Code 会写入 `.mcp.json`：

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

项目级 MCP server 首次使用时需要用户确认。不要把个人绝对路径提交给团队；团队配置可使用 PATH
中的 `cmo-bridge`，并要求每位用户按安装文档安装 wheel。

## 只安装 skill

把完整 `plugins/cmo-agent-bridge/skills/operate-cmo` 目录复制到：

```text
~/.claude/skills/operate-cmo/
```

项目级使用 `<project>/.claude/skills/operate-cmo/`。只复制 skill 不会注册 MCP server。

## 验证

在 Claude Code 中执行 `/mcp`，确认 `cmo` 已连接；然后新建会话并要求调用
`cmo_bridge_diagnose`，需要时调用 `cmo_bridge_prepare`，再调用 `cmo_bridge_status`。如果会话在
安装前已经打开，运行 `/reload-plugins` 或重启。

官方参考：[Claude Code MCP](https://code.claude.com/docs/en/mcp)、
[Plugins](https://code.claude.com/docs/en/discover-plugins)、
[Skills](https://code.claude.com/docs/en/skills)。
