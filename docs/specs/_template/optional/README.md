# Optional Templates

> 按风险启用的扩展合同，不是必填清单。默认不创建任何 optional 文件。
> 创建时从本目录复制到当前 Spec 根或 `optional/` 子目录。

## 保留

| 文件 | 何时需要 |
|---|---|
| `clarify.md` | 存在阻断当前切片的高价值产品决策 |
| `migration-design.md` | 数据库迁移、兼容窗口或回滚复杂度高 |
| `threat-model.md` | 权限 / 安全边界变化或高风险数据流 |
| `regression-map.md` | 计划把本版关键旅程晋升为长期产品回归面 |

范围、测试策略、产品摘要、Fail 归因、体验返工、UX 专有标准、调研笔记、提交清单
一律不建 optional：分别写在 `contract.md` / `tests.md` / `run.md` / `docs/product/` /
`AGENTS.md`。
