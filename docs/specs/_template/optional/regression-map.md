# Regression Map · <surface-or-spec-title>

> **产品回归地图**（长期可跑子集）。合同：插件
> `skills/testing/references/product-regression.md`  
> 关版后登记 [`docs/product/regression-register.md`](../../../product/regression-register.md)；
> 正文留 Spec（Test/Oracle 正文在 Spec，modules 只索引）。

**范围：** （从 `tests.md` 晋升的关键旅程）  
**Out：** （不挡绿；可链 GAP）  
**入口：** 宿主 `AGENTS.md` 中的回归命令（若宿主未启用产品回归，可不创建本文件）

## 分层

| 层 | 证明什么 | 命令 / 手段 |
|----|----------|-------------|
| contract | 合同预检 | |
| product | 真浏览器用户主路径 | |
| manual | 人工 | （不挡自动绿） |

## Test 覆盖表

| Test | 旅程 | 层 | 证据 | 状态 |
|----|------|-----|------|------|
| T-n | | contract / product / manual | | 回归 · Out |

## 维护

- 先改本表，再改测试标题（须含 `T-N`）  
- Out：浅 Pass / mock 单独不晋升为产品回归通过  
- 晋升后更新 `regression-register`；宿主有机读 runner 时再创建/更新 `regression/surfaces.json`
