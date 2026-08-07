# Version · <version-id>

| 字段 | 值 |
|---|---|
| **ID** | `vYYYY.MM-<slug>` |
| **标题** | |
| **状态** | `draft` |
| **Delivery Target** | `code-ready` |
| **Requirements Lock** | `open` |
| **产品决定依据** | |
| **创建日期** | YYYY-MM-DD |
| **目标环境** | |

状态只使用：
`draft | ready | in-progress | verifying | blocked | done | archived | cancelled`。

Delivery Target 只使用：
`code-ready | dev-effective | production-delivered`（见插件 workflow-contract「状态词汇」）。
`matrix-accounted` / `design-ready` / `production-restored` 不是本字段。

Requirements Lock 只使用：`open | locked | reopened`（本文为权威；Decision Cards 在
`optional/clarify.md`）。

Rail 属于对话，不属于 Version；当前 Rail 只在 `reference/handoff.md` 记录。

必填文件：`contract.md`、`tests.md`、`plan.md`、`run.md`。
按需从 `_template/optional/` 复制 `clarify` / `migration-design` / `threat-model` /
`regression-map`；默认不建空壳。
需要截图等附件时，路径写在 `run.md` Evidence 列。

## 变更记录

| 日期 | 变更 | 依据 |
|---|---|---|
| | 初版 | |
