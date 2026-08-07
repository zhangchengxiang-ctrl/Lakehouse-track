# 需求池

用户愿望、普通故障和体验反馈先记录在这里；生产事故另见
`docs/operations/incidents/`。入池 → Shape；实施以 `docs/specs/<id>/` 为准。

## 状态（条目态）

| 状态 | 含义 |
|---|---|
| `draft` | 信息可能不完整 |
| `shaping` | 正在澄清产品切片 |
| `design-ready` | **产品方案已确认**（方案闸通过），可进入研发自动编排 / Plan |
| `planned` | 已有 Spec |
| `delivered` | 已交付 |
| `parked` | 暂不处理，附理由 |

## 条目

| ID | 日期 | 类型 | 用户问题 / 目标 | 优先级建议 | 状态 | 产品真源 / Spec |
|---|---|---|---|---|---|---|
| DEM-001 | 2026-08-07 | other | Onboard：清理异仓文档并建立 SDD 文档体系 | P0 | delivered | `PRODUCT.md` / `AGENTS.md` |
| DEM-002 | 2026-08-07 | wish | 实验室一键跑通：模拟埋点 → 湖/仓可查 | P0 | shaping | `foundation/mission.md`（`make verify` 已具备，待 Spec 固化） |
| DEM-003 | 2026-08-07 | wish | 对账门 / AI 质量闸 | P2 | parked | 本期不做，范围外 |

优先级建议供 Roadmap / 产品决定参考。
