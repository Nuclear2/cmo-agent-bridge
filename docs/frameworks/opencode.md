# OpenCode

OpenCode 可在 `opencode.json` 或 `opencode.jsonc` 中定义本地 MCP server。

## MCP 配置

在用户或项目配置中合并以下内容：

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "cmo": {
      "type": "local",
      "command": ["cmo-bridge", "serve"],
      "enabled": true,
      "timeout": 120000
    }
  }
}
```

如果 OpenCode 找不到 PATH 中的命令，把数组第一项替换为 `Get-Command cmo-bridge` 返回的绝对
路径，并在 JSON 中把反斜杠写成 `\\`。

检查连接：

```powershell
opencode mcp list
```

也可运行 `opencode mcp add`，按提示选择 local server，并输入 `cmo-bridge serve`。

## Skill

把完整 `plugins/cmo-agent-bridge/skills/operate-cmo` 目录复制到任一受支持位置：

```text
~/.config/opencode/skills/operate-cmo/
~/.agents/skills/operate-cmo/
```

项目级对应 `.opencode/skills/operate-cmo/` 或 `.agents/skills/operate-cmo/`。OpenCode 也兼容
`.claude/skills`，但全局新安装建议使用 `~/.config/opencode/skills` 或通用 `.agents` 目录。

## 验证

重启 OpenCode，运行 `opencode mcp list`，然后要求 Agent 调用 `cmo_bridge_diagnose`，需要时调用
`cmo_bridge_prepare`，再调用 `cmo_bridge_status`。MCP 提供实际
工具；skill 提供使用规范，两者必须分别可见。

官方参考：[OpenCode MCP servers](https://opencode.ai/docs/mcp-servers/)、
[OpenCode Skills](https://opencode.ai/docs/skills/)。
