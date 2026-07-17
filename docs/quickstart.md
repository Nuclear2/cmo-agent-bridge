# 快速上手

本页用于把已经安装好的 bridge 跑通。完整安装和跨框架配置见
[installation.md](installation.md)。

`v0.1.4` 是 Preview / GitHub Pre-release。请在想定副本上完成首次验证。

## 1. 安装固定版本

```powershell
$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.4/cmo_agent_bridge-0.1.4-py3-none-any.whl"
uv tool install --python 3.12 $wheel
uv tool update-shell
```

重开 PowerShell 后：

```powershell
cmo-bridge --help
```

## 2. 部署 CMO 侧 Lua

```powershell
cmo-bridge prepare `
  --game-root "D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations"
```

## 3. 在想定中挂载事件

在想定编辑器里创建一个启用、可重复的 event：

- trigger：**Regular Time**，间隔 1 秒；
- action：**Lua Script**，内容为：

```lua
return ScenEdit_RunScript('CMOAgentBridge/inbox/request.lua')
```

把 trigger 和 action 关联到 event，然后保存想定。普通玩家模式不能创建该 event，因此普通推演
要使用已经由作者预置事件的想定。

保持想定时间至少以 1x 推进。暂停时 Regular Time 不触发，bridge 请求会等待。

## 4. CLI 冒烟测试

```powershell
cmo-bridge invoke bridge.status --args '{}'
cmo-bridge invoke scenario.get --args '{}'
```

成功输出包含：

```json
{
  "ok": true
}
```

返回超时时，确认 CMO 正在运行、打开的是已挂载事件的想定、event 已启用且可重复，以及想定时间
正在推进。

## 5. 连接 Agent

MCP server 的标准启动命令：

```text
cmo-bridge serve
```

按使用方式选择配置：

- 只有 ChatGPT / Codex Desktop：使用 [Codex Desktop 本地安装脚本](frameworks/codex.md#只有-chatgpt--codex-desktop)；
  不需要 Codex CLI，但仍需要 `uv` / `uvx`。
- 有 Codex CLI：注册 `stable` marketplace，再用 `codex plugin add` 或 `/plugins` 安装；后续发布会在
  客户端启动时自动刷新。
- Claude Code：安装 `stable` marketplace plugin；plugin 同时携带 MCP 与完整 Skill。第三方
  marketplace 默认不自动更新，需要在 `/plugin` 中为 `cmo-tools` 启用 auto-update。
- [OpenCode](frameworks/opencode.md)、[Cursor](frameworks/cursor.md)、[Qoder](frameworks/qoder.md) 和
  [通用 MCP](frameworks/generic-mcp.md)：既要注册 MCP，也要安装完整的 `operate-cmo` Skill。MCP
  server 本身不会分发 Skill。

重启 Agent 并新建会话，然后先要求它调用 `cmo_bridge_diagnose`；需要时由它在当前会话调用
`cmo_bridge_prepare`，再调用 `cmo_bridge_status`。当前 MCP surface 提供 70 个有类型
工具。CLI 只用于安装、诊断和人工测试；Agent 正常工作应调用 `cmo_*` tools。

## 6. 选择正确模式

- 默认 `LIVE_PLAYER`：只用己方状态和己方观察到的 contacts，适合公平推演；
- 用户明确要求制作想定时才切换 `SCENARIO_AUTHOR`；
- 明确的测试、注入或裁决使用 `UMPIRE`。

复杂多步操作前先降至 1x、刷新态势、执行并读回校验，再恢复原时间倍率。不要因为 MCP 技术上
可以读取全知信息，就把敌方真实单位状态用于 `LIVE_PLAYER` 决策。

## 一个安全的首次任务

向 Agent 发出：

```text
以 LIVE_PLAYER 模式连接当前想定，只调用只读工具：检查 bridge 状态，列出阵营，识别我方阵营，
然后基于我方 contacts 和 missions 给出态势摘要。不要进行任何修改。
```

确认只读链路稳定后，再尝试创建一个初始为 inactive 的任务、读回校验，最后决定是否激活。
