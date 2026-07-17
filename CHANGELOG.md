# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/)。

## [0.1.0] - 2026-07-17 (Preview)

首个公开预览版，以 GitHub Pre-release 发布。该版本用于早期实机验证和兼容性反馈，不承诺稳定版
兼容性；使用写操作前应保存想定副本。

### Added

- 本地 `stdio` MCP server，共 68 个有类型的 `cmo_*` tools。
- 基于 CMO Lua 与本机文件交换的双向 bridge；主机状态使用本地 SQLite 管理。
- 想定、阵营、单位、接触、任务、条令、WRA、EMCON、传感器、武器、库存和计分读取。
- 任务创建/更新、任务区、相对参考点、单位分配、飞行计划、TOT/起飞时刻和任务级空中加油规划。
- 发射、返航、加油、攻击、航路、传感器、载荷、货运与时间倍率控制。
- 想定作者工具：天气、时间线、阵营、姿态、事件组件、Special Action、计分、单位与任务管理。
- LuaScript event component 与 Special Action authoring 可承载 Lua；经激活或执行后具有本机 CMO
  进程内代码执行能力，仅限可信 `SCENARIO_AUTHOR`/`UMPIRE` 工作流。
- 破坏性单位/任务删除的 preview-and-confirm 流程，以及不确定写入的 host quarantine 处理。
- 适用于高速时间倍率下文件更新的响应一致性与竞态处理。
- `operate-cmo` skill，明确 `LIVE_PLAYER`、`SCENARIO_AUTHOR`、`UMPIRE` 的信息与权限边界，并提供
  海空作战规划、推演和想定制作流程。
- `uv` 隔离安装、Release wheel、CLI、Codex/Claude marketplace 以及 Codex、Claude Code、
  OpenCode、Cursor、Qoder 和通用 MCP 配置文档。

### Known limitations

- 当前不提供确定性的暂停、启动或单步推进；复杂操作使用 1x 时间倍率。
- 不提供单调用的通用 `lua.eval`/`lua.call`；想定作者的 Lua-bearing event/Special Action 仍可组合
  执行任意 CMO Lua，必须逐行审查、保存副本、默认 inactive、读回核对后再启用。
- 自动多任务分配队列、生成后航路点编辑、operation planner 全字段和完整 zone object 编辑尚未覆盖。
- 已验证 CMO Build 1868；其他 build 需要重新进行兼容性验证。

[0.1.0]: https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.1.0
