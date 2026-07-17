# Security Policy

## 支持版本

| 版本 | 安全更新 |
|---|---|
| 0.1.x | 支持 |
| 更早版本 | 不支持 |

## 报告漏洞

请使用 GitHub 仓库的 **Security > Report a vulnerability** 私密报告渠道：

<https://github.com/Nuclear2/cmo-agent-bridge/security/advisories/new>

不要在公开 issue 中披露尚未修复的漏洞。报告应包含受影响版本、复现步骤、预期与实际结果、潜在
影响，以及可安全分享的日志。请删除想定名称、玩家信息、绝对路径和其他不必要的私人数据。

如果 GitHub 私密报告不可用，可先提交一个不包含漏洞细节的普通 issue，请维护者开启私密沟通。

## 安全边界

CMO Agent Bridge 是一个**受信任的同机工具**：

- MCP server 使用 `stdio`，不监听 TCP 端口，也不提供远程认证层；
- Python 主机只应由当前 Windows 用户的 Agent 客户端启动；
- Lua dispatcher 在 CMO 进程内运行，能够按照有类型的操作修改当前想定；
- 主机与 CMO 通过 game root 和 `%LOCALAPPDATA%\CMOAgentBridge` 下的文件通信；
- 默认允许普通 mutation，但 destructive 操作默认关闭；支持的删除还要求 preview-and-confirm；
- bridge 没有单调用的通用 `lua.eval`/`lua.call` 工具；但 `SCENARIO_AUTHOR`/`UMPIRE` 可用的
  LuaScript event component 与 Special Action 能保存 Lua，组合激活或执行后等价于在本机 CMO
  进程内执行代码。

这不是为多用户服务器、互联网暴露、提权边界或不受信任本地用户隔离设计的系统。不要通过网络
管道转发 stdio，不要让不受信任用户写入 bridge 目录，也不要在高权限账号下运行不受信任的 Agent。

## 用户责任

- 只从本仓库和 GitHub Release 安装，并在可能时核对 Release 校验和；
- 在写操作前保存想定副本，尤其是想定制作、库存调整和删除操作；
- 逐行审查 Agent 的 tool call、skill 和全部 Lua 源码；Lua-bearing event/Special Action 应默认
  inactive 且不可重复，读回核对完整脚本和链接后才允许启用；
- 只允许可信作者在 `SCENARIO_AUTHOR`/`UMPIRE` 中创建、修改或测试 Lua-bearing artifact；
  `LIVE_PLAYER` 只能执行想定原本提供的合法玩家 Special Action，不得查看或替换脚本；
- `LIVE_PLAYER` 下不要授予或使用全知敌方信息；这属于推演完整性问题，即使不是系统安全漏洞；
- 遇到 timeout 或不确定写入时不要盲目重试，应先读取结果或处理 quarantine；
- 不要提交 CMO 安装文件、商业想定、数据库、日志或包含个人路径的状态文件。

Matrix Games、Slitherine、WarfareSims 和 CMO 不为本项目提供安全保证或支持。
