# Agent 框架配置

CMO Agent Bridge 的执行层是标准本地 `stdio` MCP server。不同框架的主要差异只在配置文件结构、
命令行入口以及 skill/plugin 的发现目录。

| 框架 | MCP 注册 | operate-cmo skill | Marketplace plugin |
|---|---|---|---|
| Codex | `codex mcp add` 或 `~/.codex/config.toml` | plugin 自动携带；也可放入 `.agents/skills` | Desktop / CLI 支持；IDE 不支持 plugin |
| Claude Code | `claude mcp add` 或 `.mcp.json` | plugin 自动携带；也可放入 `.claude/skills` | 支持 |
| OpenCode | `opencode.json` | 全局 `~/.config/opencode/skills`；项目 `.opencode/skills` | 本项目不依赖 |
| Cursor | `~/.cursor/mcp.json` 或项目 `.cursor/mcp.json` | `.cursor/skills` 或 `.agents/skills` | MCP/skill 手工配置 |
| Qoder | `qodercli mcp add` 或 `.mcp.json` | `.qoder/skills` | MCP/skill 手工配置 |
| 其他客户端 | 客户端的 stdio MCP 配置 | 取决于是否实现 Agent Skills | 不要求 |

## 推荐组合

1. 安装 `uv`；MCP 启动后让 Agent 调用 `cmo_bridge_diagnose` 和所需的 `cmo_bridge_prepare`；
2. 在想定中保存 1 秒轮询事件；
3. 用框架原生方式注册 `cmo-bridge serve`；
4. 安装完整 `operate-cmo` skill；
5. 重启框架并新建会话，调用 `cmo_bridge_diagnose`，然后用 `cmo_time_get_state` 检查时间状态：运行中
   直接调用 `cmo_bridge_status`；已暂停则调用 `cmo_simulation_pulse(handshake=true)`，自动完成握手
   并重新暂停。

Codex 或 Claude Code 用户可以用 marketplace plugin 同时取得 MCP 配置与 skill。不要再手工注册同名
server，否则同一会话可能加载两套相同工具。

所有框架看到的 v0.3.1 工具契约一致：普通 mutation 返回 `QueuedOperationReceipt`，Agent 再用
`cmo_request_get` 或 `cmo_request_wait` 获取最终结果。CMO 暂停不会丢失 durable request；等待超时
也不会取消它。时间控制也采用相同的 `cmo_time_get_state`、`cmo_time_set` 和
`cmo_simulation_pulse` 契约。默认保持当前倍率，普通指令不暂停；pulse 只用于已暂停的握手或复杂
规划期间让队列工作生效；调用时必须包含所有当前非终态请求的 UUID。它会复停并恢复原倍率，
但不提供零时间单步。时间 UI 操作本身不依赖 Lua，握手和队列执行仍依赖 Regular Time 轮询。
完整的依赖、取消和恢复规则由 `operate-cmo` Skill 提供。

## PATH 与绝对路径

先运行：

```powershell
(Get-Command cmo-bridge).Source
```

命令行示例可直接使用 `cmo-bridge`。如果桌面 Agent 无法从 PATH 找到它，把上面返回的完整
`cmo-bridge.exe` 路径写进配置；JSON 中的 `\` 要写成 `\\`。更新 PATH 后应完全重启桌面应用。

## 分页

- [Codex](codex.md)
- [Claude Code](claude-code.md)
- [OpenCode](opencode.md)
- [Cursor](cursor.md)
- [Qoder](qoder.md)
- [通用 MCP 客户端](generic-mcp.md)
