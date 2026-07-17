# 快速上手

本页用于把已经安装好的 bridge 跑通。完整安装和跨框架配置见
[installation.md](installation.md)。

`v0.2.0` 是 Preview / GitHub Pre-release。请在想定副本上完成首次验证。

## 1. 安装固定版本

```powershell
$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.2.0/cmo_agent_bridge-0.2.0-py3-none-any.whl"
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

同步读取和首次状态握手时，保持想定时间至少以 1x 推进。暂停时 Regular Time 不触发；普通写操作
可以先进入本地持久队列，并在时间恢复后执行。

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

纯 CLI 写操作使用 `cmo-bridge submit`；它只持久化请求，不启动 worker。没有 MCP server 时，随后
运行 `cmo-bridge request-wait <request-id>` 才会在前台启动 worker。等待超时不取消请求，再次
`request-wait` 会继续处理。

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
`cmo_bridge_prepare`，再调用 `cmo_bridge_status`。当前 MCP surface 提供 75 个有类型
工具。CLI 只用于安装、诊断和人工测试；Agent 正常工作应调用 `cmo_*` tools。

## 6. 处理写操作回执

普通 mutation 工具会立即返回 `QueuedOperationReceipt`，不会在同一次调用中返回 CMO 的最终结果。
保存其中的 `request_id`，再调用：

- `cmo_request_get`：查看一个请求的当前状态和最终结果；
- `cmo_request_wait`：等待指定秒数；等待超时只结束这次等待，不会取消请求；
- `cmo_request_list` / `cmo_queue_status`：查看队列；
- `cmo_request_cancel`：只取消仍为 `queued` 的请求，不能撤销 `active` 请求。

独立修改可连续提交，并按 FIFO 顺序执行。需要上一步返回 GUID 的操作必须等上一步
`completed` 后再提交。CMO 暂停时 mutation 会持续保留；恢复时间后自动执行。读取和 status 仍是
同步 CMO 调用，暂停时可能超时。MCP 客户端退出不会取消 `active` 请求，重启后会继续恢复；如果
进程或想定 binding 已变化，旧请求会被拒绝或隔离，而不会跨想定执行。

## 7. 选择正确模式

- 默认 `LIVE_PLAYER`：只用己方状态和己方观察到的 contacts，适合公平推演；
- 用户明确要求制作想定时才切换 `SCENARIO_AUTHOR`；
- 明确的测试、注入或裁决使用 `UMPIRE`。

复杂多步操作前先降至 1x、刷新态势、提交写操作并取得最终结果，再读回校验和恢复原时间倍率。
如果用户主动暂停，也可先排入独立写操作，待时间释放后完成。不要因为 MCP 技术上
可以读取全知信息，就把敌方真实单位状态用于 `LIVE_PLAYER` 决策。

## 一个安全的首次任务

向 Agent 发出：

```text
以 LIVE_PLAYER 模式连接当前想定，只调用只读工具：检查 bridge 状态，列出阵营，识别我方阵营，
然后基于我方 contacts 和 missions 给出态势摘要。不要进行任何修改。
```

确认只读链路稳定后，再尝试创建一个初始为 inactive 的任务；等待创建请求完成、取得任务 GUID，
读回校验后再决定是否提交激活请求。
