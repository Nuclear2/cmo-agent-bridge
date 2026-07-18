# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/)。

## [Unreleased]

## [0.2.1] - 2026-07-18 (Preview)

这一版让 Agent 在首次部署前读取保存的想定介绍和当前玩家方简报，先确认战役目标与已知态势，
再进行作战规划。

### Added

- 新增 `cmo_scenario_context_get`：读取保存的想定介绍、当前玩家方 briefing 及五档胜负分数阈值；
  工具不会接受任意阵营参数，也不会解密其他阵营的简报。
- 支持解析 briefing 中的本地 `[LOADDOC]` 文档，并同时给 Agent 返回便于规划的纯文本。

### Changed

- `cmo_scenario_get` 增加想定文件位置投影，供宿主侧读取当前保存的 `.scen` 或 `.save`。
- `operate-cmo` 现在要求 Agent 在首次部署前读取想定介绍和当前方简报，提取任务、终态、时限、
  ROE、硬约束、已知态势与胜负标准；拿不到时不得自行臆造战役目标。
- 项目版本升级到 `0.2.1`。

## [0.2.0] - 2026-07-18 (Preview)

这一版把普通 CMO 写操作改为本地持久异步队列。Agent 可以在 CMO 暂停时先提交命令，待想定
时间恢复后按顺序执行；MCP 重启也不会丢失已进入执行阶段的请求。

### Added

- 新增基于 SQLite 的 durable FIFO mutation queue，并加入 `cmo_request_get`、
  `cmo_request_wait`、`cmo_request_list`、`cmo_request_cancel` 和 `cmo_queue_status`；MCP surface
  增至 75 个有类型工具。
- 写请求保存当前 CMO process、runtime 和 scenario binding。重启后会核对 ledger、inbox 与结果并
  恢复；binding 不匹配时拒绝或隔离旧请求，不会跨进程或跨想定执行。
- `prepare` 在改写 Lua runtime 前会拒绝非空的 queued/active 工作或尚未收敛的 pending journal，
  避免升级覆盖正在执行的请求。

### Changed

- 普通 mutation 工具现在立即返回 `QueuedOperationReceipt`。调用方必须用回执中的 `request_id`
  查询或等待最终 CMO 结果；依赖创建结果 GUID 的下一步必须等前一步完成后再提交。
- CMO 暂停或轮询暂时停止时，active mutation 不再按旧的调用超时撤回，而是无限保持 pending，
  时间恢复后继续。关闭 MCP/client 只会让 worker 脱离，不会取消已经 active 的请求。
- `cmo_request_wait` 的 `timeout_seconds` 只限制本次等待，不改变请求状态。只有仍为 `queued` 的请求
  可以取消；已经 `active` 的请求不能承诺中止。
- 纯 CLI `submit` 只持久化 mutation；`request-wait` 在前台启动 worker 并在完成或本次等待超时后
  停止。正常 MCP server 仍持续托管 worker。
- 读取、bridge status、host prepare 和破坏性删除的 preview/confirm 仍保持同步契约；同步读取在
  CMO 暂停或轮询停止时仍可能按原有超时返回。
- 项目版本升级到 `0.2.0`。

## [0.1.4] - 2026-07-17 (Preview)

这一版补齐当前玩家阵营识别，并明确暂停期间请求等待、恢复轮询后继续执行的工作流。

### Added

- `cmo_scenario_get` 新增必填但可为空的 `player_side_guid`，直接投影官方
  `VP_GetScenario().PlayerSide`；Lua 往返、MCP 输出 schema 和严格响应模型同步覆盖。

### Changed

- `operate-cmo` 在每个新想定的首次 `LIVE_PLAYER` 周期中，必须将 `player_side_guid` 与完整
  `cmo_side_list` 匹配并报告指挥方名称与 GUID；无法唯一匹配时禁止猜测阵营或执行写操作。
- 文档明确暂停期间已排队的请求会在有界等待窗口内继续等待，轮询恢复后正常完成；Skill
  同时说明应保持 1x 直到工具返回，或按需重复 `Alt+1` 的 15 秒单步。
- 项目版本升级到 `0.1.4`。

### Fixed

- 状态握手超时现在明确列出“想定暂停”和“轮询事件未激活或未加载”两类可能原因及恢复步骤，
  不再把其中任一原因当作已确认事实。

## [0.1.3] - 2026-07-17 (Preview)

这一版把插件市场的默认安装入口切换到 `stable` 发布通道，并为 Codex 插件和 `operate-cmo` Skill 加上 CMO 图标。

### Added

- Codex 插件与 `operate-cmo` Skill 现在使用游戏安装目录中的 CMO 图标；发布包内同时附带来源与权利说明。

### Changed

- Codex 和 Claude Code 的 marketplace 安装命令改为跟踪 `stable`，无需在每次升级时重新注册新的版本标签。
- Release 工作流只在 wheel、插件包和校验文件全部发布成功后推进 `stable` 分支；既有版本标签仍保持不可变。
- 项目版本升级到 `0.1.3`。

## [0.1.2] - 2026-07-17 (Preview)

本次预览版补齐 MCP 首次启动与版本升级后的自恢复流程；即使 CMO 侧 runtime 尚未部署，Agent
也能先加载工具并在当前任务内完成准备。

### Added

- 新增 host-only `cmo_bridge_diagnose` 与 `cmo_bridge_prepare`，MCP surface 增至 70 个有类型工具。
- `cmo_bridge_prepare` 成功后会在同一 stdio 进程和同一 MCP 会话中启用普通 CMO 工具，无需再次
  注册工具或重启 Agent。

### Changed

- `cmo-bridge serve` 改为延迟构建严格 CMO runtime；无配置、runtime 缺失或版本不匹配时不再在
  MCP initialize 之前退出。
- `operate-cmo` 优先使用 MCP 内诊断与准备工具；只有工具本身缺失时才进入 `uvx`/客户端启动排障。

### Fixed

- `operate-cmo` 现在区分 MCP 启动失败、host runtime 未准备与 CMO 轮询超时，并在各层给出可执行
  的下一步和轮询事件挂载说明。
- Codex plugin 改用标准根目录 `.mcp.json`，并将首次 `uvx` 冷启动超时提高到 60 秒；Claude Code
  继续使用不含 Codex 专属超时字段的独立配置。

## [0.1.1] - 2026-07-17 (Preview)

本次预览版集中完善跨框架发行与安装体验，不改变 CMO bridge 的工具协议。

### Added

- 提供面向 Codex Desktop 的本地安装脚本，并将其作为独立 GitHub Release 资产发布。

### Fixed

- 为 Codex 和 Claude Code 分别提供符合各自格式的 MCP 配置；Codex 使用直接 server map，
  Claude Code 继续使用带 `mcpServers` 的配置。
- 修正 Codex marketplace 的说明，不再暗示尚未注册的自建 marketplace 会自动出现在插件目录中。

### Changed

- 明确 Codex/Claude plugin 同时包含 MCP 启动配置和完整 `operate-cmo` skill；仅安装 wheel 或注册
  MCP 的其他框架仍需单独安装 skill。
- 重写快速开始与安装说明，并在 README 开头醒目标明当前仅支持 Windows。

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

[0.2.1]: https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.2.1
[0.2.0]: https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.2.0
[0.1.4]: https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.1.4
[0.1.3]: https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.1.3
[0.1.2]: https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.1.2
[0.1.1]: https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.1.1
[0.1.0]: https://github.com/Nuclear2/cmo-agent-bridge/releases/tag/v0.1.0
