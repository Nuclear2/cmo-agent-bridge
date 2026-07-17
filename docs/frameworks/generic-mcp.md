# 通用 MCP 客户端

任何支持本地 `stdio` transport 和 MCP tools 的客户端都可以启动 CMO Agent Bridge。

## 最小配置

常见 JSON 结构如下：

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

有些客户端省略 `type`，有些把命令写成数组。保持最终启动的进程等价于：

```text
cmo-bridge serve
```

该进程的 stdout 只用于 MCP 协议，不能被启动脚本的调试文字污染。若客户端不继承 PATH，使用
`(Get-Command cmo-bridge).Source` 返回的绝对路径。

## uvx 版本固定配置

客户端也可以不持久安装 wheel，直接启动：

```json
{
  "mcpServers": {
    "cmo": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "--python",
        "3.12",
        "--from",
        "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.1/cmo_agent_bridge-0.1.1-py3-none-any.whl",
        "cmo-bridge",
        "serve"
      ]
    }
  }
}
```

长期使用推荐先 `uv tool install`，再用更短的配置。无论哪种方案，都必须先运行一次
`cmo-bridge prepare`（或等价的 `uvx ... cmo-bridge prepare`），并在想定内挂载轮询事件。

## 客户端能力要求

- 支持 MCP `tools/list` 和 `tools/call`；
- 能启动本机进程并保持双向 stdio；
- 工具超时建议至少 120 秒；
- server 启动超时建议至少 30 秒；
- 一次会话只启动一个 bridge server，避免相同工具重复注册。

## 安装 Skill

注册 MCP 只会提供 `cmo_*` 工具，不会告诉 Agent 如何评估态势、组织任务或校验操作。Agent Skills
不是 MCP 协议的一部分，因此必须另外安装完整的 `operate-cmo` Skill。

下载并解压独立 Skill 包：

```powershell
$skillZip = Join-Path $env:TEMP "operate-cmo-skill-0.1.1.zip"
$skillRoot = Join-Path $env:TEMP "operate-cmo-skill-0.1.1"
Invoke-WebRequest `
  -UseBasicParsing `
  -Uri "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.1/operate-cmo-skill-0.1.1.zip" `
  -OutFile $skillZip
Expand-Archive -LiteralPath $skillZip -DestinationPath $skillRoot -Force
```

把解压后的整个 `$skillRoot\operate-cmo\` 目录安装到客户端的 Skill 搜索路径；它已包含
`SKILL.md`、`agents/` 和 `references/`。如果客户端不支持 Agent Skills，可把这些说明作为
系统/项目指令导入，但不要声称客户端已经原生安装 Skill。
