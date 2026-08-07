# 产品文档

本目录保存相对稳定的产品目标、能力蓝图和差距；版本实施以 `docs/specs/<id>/` 为真源，
实际行为以代码和运行环境为真源。

## 读序

| 路径 | 回答什么 |
|---|---|
| `foundation/` | 使命、原则、角色旅程、系统边界 |
| `DESIGN.md` | 可选：默认 surface、锚点、品牌色/字体、反参考（插件 design-standards 底线之上） |
| `modules/<slug>/` | 成块能力的产品蓝图（章节合同见插件 skill `design` → `product-package`） |
| `demand-pool.md` | 尚未进入 Plan 的愿望和反馈 |
| `gap-register.md` | 蓝图与现状之间的差距 |
| `regression-register.md` | 维护态关键用户旅程（可选；合同见 skill `testing` → `product-regression`） |
| `../planning/roadmap.md` | 排期 |
| `../specs/<id>/` | 当前版本实施合同 |

## 能力地图

| ID | 能力 | 蓝图包 | 状态 | Spec / Gap |
|---|---|---|---|---|
| C-001 | 埋点采集 → 数据湖 → 数据仓库 | `foundation/mission.md` | partially-delivered | GAP-001 |

能力态（本表）：`shaping | design-ready | planned | partially-delivered | accepted | archived`。  
demand 条目态见 `demand-pool.md`，与本表分表维护（不同行 / 不同文件）。

## 三池流转

```text
demand-pool（愿望）──Shape 确认方案──► design-ready
        │                                    │
        │                            用户批准进入 Plan
        │                                    ▼
        │                         docs/specs/<id>/（实施合同）
        │                                    │
gap-register（蓝图 vs 现状差距）──关闭──► gap-closed（审计）
```

| 池 | 何时写入 | 何时流出 |
|---|---|---|
| `demand-pool.md` | Shape：新愿望 / 故障 / 体验反馈 | 切片确认 → `design-ready`；批准 Plan 后挂 Spec → `planned` |
| `gap-register.md` | 蓝图与代码/运行现状不一致 | 差距消除或明确不做 → `gap-closed.md` |
| `gap-closed.md` | 从 gap-register 关闭的条目 | 归档审计 |

新愿望进 demand-pool；实施与验收以 `docs/specs/<id>/` 为真源。产品决定变化时更新蓝图，
技术实现变化时更新 Spec 或代码。
