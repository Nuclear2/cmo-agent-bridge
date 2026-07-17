# Contributing

感谢参与 CMO Agent Bridge。项目优先保证三件事：有类型且可审计的操作、真实 CMO build 上的行为
校验，以及 `LIVE_PLAYER` 与想定作者权限的清晰边界。

## 开发环境

- Windows 10/11；
- `uv`；
- Python 3.12（由 `uv` 管理）；
- 只有 `cmo_live` 测试需要本机安装并由用户控制的 CMO。

使用正常 Git clone：

```powershell
git clone https://github.com/Nuclear2/cmo-agent-bridge.git
cd cmo-agent-bridge
uv sync --locked
```

Git worktree 管理目录、`.venv`、`.uv-cache`、测试临时目录和本机 CMO 文件都不是源码的一部分，
不要加入提交。

## 修改前

1. 先搜索现有 issue 和 pull request；
2. 对新的 Lua API wrapper，引用官方 [Command Lua API](https://commandlua.github.io/)；
3. 不要只根据文档示例推断唯一参数形态。CMO wrapper 经常返回 table/userdata，先探查真实字段；
4. 明确操作属于读取、普通 mutation、破坏性操作还是 scenario-authoring；
5. 为非幂等操作设计 discover-before-retry 和可验证 readback。

## 本地检查

```powershell
uv run --locked ruff check .
uv run --locked pyright
uv run --locked pytest -m "not cmo_live"
```

涉及 Lua dispatcher、协议、文件交换或 MCP schema 时，还应运行对应 unit、contract 和 integration
测试。只有用户明确打开测试想定并允许操作时，才运行 `cmo_live` 测试。

## 添加或修改工具

- 使用专用、类型化的 operation；不要添加任意 Lua escape hatch；
- 参数只表达允许改变的字段，避免隐式覆盖 CMO wrapper 的其他状态；
- 读工具支持必要的分页、GUID 选择和稳定投影；
- 普通写工具返回 durable queue receipt；调用方通过 request ID 取得最终 CMO 结果，并继续区分
  “命令已接受”与“模拟效果已生效”；
- `cmo_request_wait` 超时只能结束本次等待，不能取消、重排或重复提交 durable request；
- 只允许取消尚未 active 的 queued request；重启恢复必须核对 process/runtime/scenario binding；
- 想定作者工具在 skill 和工具说明中标为 `SCENARIO_AUTHOR`/`UMPIRE`；
- 更新生成的 operation manifest、schema corpus、tool catalog、测试和变更记录。

## 真实 CMO 验证

CMO Lua 文档的示例不是完整契约。新能力至少验证：

1. 最小有效输入和真实返回字段；
2. 无效输入的有界失败；
3. 普通玩家与编辑器上下文差异；
4. 修改后的立即 readback；
5. 时间推进后的实际行为；
6. 保存、重载和再次读取；
7. 重名对象、分页和 GUID 选择；
8. 高时间倍率下的文件交换；
9. 依赖消失、CMO 暂停、worker 重启和 binding 变化时的队列恢复路径。

测试记录必须注明 CMO build。不要向仓库提交商业想定、数据库、游戏二进制或不具备再分发权的素材。

## 文档和发行

- 面向用户的命令以 Release wheel 和正常仓库根目录为基准；
- 跨框架示例应保持等价的 `stdio` 命令 `cmo-bridge serve`；
- 区分 MCP（执行）、skill（知识/流程）、plugin（发行包装）和 CLI（安装/诊断）；
- 发布前重新构建 wheel，验证 plugin/skill，检查 wheel 与源码版本一致，并更新 `CHANGELOG.md`。

## Pull request

PR 应包含：问题与边界、实现摘要、测试证据、真实 CMO build 证据（如适用）、文档变化和已知限制。
保持提交聚焦，不要顺带格式化无关文件。安全问题请按照 [SECURITY.md](SECURITY.md) 私密报告。
