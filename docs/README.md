# SDD 文档地图

这些文档是 Agent 跨任务的交付记忆。用户只需确认产品方向、单向门和真实结果。

## 权威来源

| 事实 | 唯一真源 |
|---|---|
| 项目命令、环境、架构、红线、就绪度 | `AGENTS.md` |
| 社区设计底线（系统架构 / UX / 视觉） | 插件 `design-standards/`（AGENTS 可覆盖） |
| 可选 as-built 边界与 ADR | 仓库 `docs/architecture/`（非 SDD 保留路径） |
| 实验室运行手册 | `docs/ops/`（宿主运维，非 SDD 保留路径） |
| 产品愿望与长期蓝图 | `product/` |
| 当前版本实施合同 | `specs/<id>/`（`contract` + `tests` + `plan` + `run`） |
| 活跃任务、Workspace、PR 和下一步 | `reference/handoff.md`（索引，不复制合同正文） |
| 并行互斥 Claim | `reference/claims.md` |
| 实际实现 | 代码、迁移和配置 |
| 验收证据 | `specs/<id>/run.md`（附件路径写在 Evidence 列） |
| 生产事故 | `operations/incidents/` |

同一规则只维护一处；其他文档只链接权威来源。

### SDD 保留路径（独占语义）

下列路径由 SDD 占用；宿主原有技术文档请放在其他子树（例如 `docs/architecture/`、`docs/api/`），或迁到 `docs/_host/`：

| 路径 | 语义 |
|---|---|
| `product/` | 愿望、蓝图、gap（含 `modules/`、`foundation/`） |
| `specs/` | 实施合同与 `_template/` |
| `reference/handoff.md` · `reference/claims.md` | 交付索引与 Claim |
| `planning/roadmap.md` | 排期（full scaffold；minimal 可后补） |
| `operations/incidents/` | 事故记录 |

存量初始化：优先 `make scaffold PROFILE=minimal`（或默认 `detect`）。硬冲突时 scaffold 会 `BLOCK`；也可 `SDD_ROOT=docs/sdd`（或 `--root=docs/sdd`）把 SDD 挂到子树，并在 `AGENTS.md` 盖章 `SDD docs root`（见插件 `skills/vibe-coding/references/docs-root.md`）。

旧 wiki / README **只链接、不整页搬进** `product/`，避免双真源。

### 真源优先级（冲突时）

```text
specs/<id>/（执行合同）+ 代码/运行环境
  ≫ reference/handoff.md
  ≫ product/modules/（产品蓝图）
  ≫ product/demand-pool.md（愿望）
```

状态词汇（Version / Delivery Target / Spec Run / 完成声明）以插件
`skills/vibe-coding/references/workflow-contract.md`「状态词汇」为**唯一真源**；
本目录不复述枚举。

## 工作轨

```text
Shape → Plan → Build(Spec) → Verify
                      ↑         |
                      └ Repair ─┘

复杂/线上问题：Diagnose → Repair | Plan | Incident
生产事故：Incident → Production Verification → 长期执行合同
```

- Shape 澄清产品诉求；Plan 形成技术方案、**完整测试用例**和 Spec 执行合同；
- 每个工作轨在自身范围内连续完成；跨工作轨前先总结并等待用户批准；
- Verify 只验收、记录证据和分类 Fail（不改 `tests.md` Oracle）；
- Diagnose 只定位，Incident 只恢复生产。

## Spec 布局与读序

```text
docs/specs/<id>/
├── VERSION.md
├── contract.md   # WHAT
├── tests.md      # TEST
├── plan.md       # HOW
└── run.md        # RUN
```

读序：`VERSION` → `contract` → `tests` → `plan` → `run`（Build 起）。

## 检查

可选：从插件仓维护者工具校验宿主 docs：

```bash
bash <plugin>/evals/tools/check_docs.sh .
```

脚手架只在用户明确要求时运行，不因缺少目录自动写入宿主仓库。
