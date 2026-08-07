# Plan · <version-id>

## 方案差量

> 相对 `contract.md` 事实映射：只写本版要改什么，不复述已 Verified 现状。

- 核心机制：
- 组件 / 模块影响：
- 数据与 API：
- 权限与安全：
- 可观测性：

## 架构与设计边界

> 插件 `design-standards`：触及新入口 / 跨层 / 新存储 / 新权限模型 / 新部署单元时必填；
> 否则写「沿用现有边界，无架构差量」。有 UI 时对照 ux + visual。

- 边界：沿用 / 新开（说明）：
- C4 影响：L1 / L2 / L3 / N/A：
- ADR：否 / 是（路径）：
- UX / 视觉约束（或 N/A）：
- Unverified：

## 纵向切片（默认执行轴）

> 按入口拆，不按 root/resolver/ACL 等横向层拆。Codex 普通回合默认一次只交付一个切片。

| 切片 ID | 入口 | 完成定义（链 T-xxx） | 触及路径 | 依赖 | 备注 |
|---|---|---|---|---|---|
| S1 | | T-001, T-002 | | | |

> **触及路径**：本片建议 Codex 先读的 3–8 个路径（逗号分隔）。用于 Context Pack；可空（则仅 Spec 指针）。  
> 旧五列表（无触及路径列）仍可被 `build_context_pack.py` 解析。

共享 helper / 迁移：仅当 ≥2 切片 Verified 重复同一逻辑，或某切片证明现有关系无法解析时才新增。

## 备选与取舍

| 方案 | 收益 | 代价 / 风险 | 结论 |
|---|---|---|---|
| | | | |

## 单向门

| 项 | 是否触发 | 审批 | 附加验证 |
|---|---|---|---|
| 数据库迁移 | no | N/A | |
| 对外契约 | no | N/A | |
| 权限 / 安全 | no | N/A | |
| 数据删除 | no | N/A | |
| 生产配置 / 部署单元 | no | N/A | |

## 迁移与回滚

- 兼容策略：
- 发布顺序：
- 回滚点：
- 数据恢复：

> 复杂度高时另建 `optional/migration-design.md`；此处保留摘要与链接。

## Workspace Strategy

> 本 Spec Build 的隔离与交付边界；字段语义以插件 `workspace-contract.md` 为唯一真源。
> 宿主默认分支、setup、分支保护只读 `AGENTS.md`。`claims` 只列外部共享资源。

```yaml
owner:
  kind: current-chat | user-thread | subagent
  id: "<native-id-or-local>"

workspace:
  mode: local | managed-worktree | git-worktree
  base_ref: "<host-default-branch>"
  base_sha: "<confirmed-sha>"
  branch: sdd/<spec>
  setup: "<host-defined>"
  shared_resources: []
  claims: []

native:
  plan_item: null
  goal_id: null

delivery:
  target: local-diff | commit | push | draft-pr | ready-pr
  pr_base: "<host-default-branch>"
  merge_after: []
  merge_before: []
  integration_retest: required | n/a
```

## 真实 Blocker 与授权

> 仅 Verified。运行态与批次结果写在 `run.md`，此处不复制。

- 纵向切片实施顺序：
- 外部授权：
- 真实 Blocker：

## 未决技术问题

| 问题 | 是否阻断 | 负责人 | 截止条件 |
|---|---|---|---|
| | | | |
