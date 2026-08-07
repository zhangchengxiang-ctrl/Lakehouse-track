# Run · <version-id>

> 本 Spec 的唯一运行态：批次结果、Fail/Repair 与关版结论写在此文件。
> 预期 Oracle 在 `tests.md`；此处只记实际结果与证据路径（附件按需落盘）。

## 执行态

- 状态：`ready | building | unit-testing | verifying | repairing | blocked | awaiting-human-acceptance | acceptance-passed`
- 当前模式：`build | verify | repair`
- Owner / Workspace：
- 最后更新：
- 允许结束条件：`awaiting-human-acceptance | acceptance-passed | blocked | needs-authorization`

### Build

- 已覆盖入口：
- 未覆盖入口：
- 实现冻结时间（开始单元测试前）：
- oracle-freeze: `intact`（禁改 `tests.md` / 验收矩阵；改 Oracle 须回 Plan）
- 红绿证据: `red <cmd> exit=<n> · green <cmd> exit=0` | `N/A · polish|trivial|无自动化`

## 结果

> 测试开始后冻结改码；先收齐结果。有自动化则先红后绿，退出码写入上方「红绿证据」。

### 批次结果

| 层或命令 | 覆盖 Tests | 结果 | 证据（`kind=` · 路径） |
|---|---|---|---|
| | T-001 | Pass / Fail / Blocked | kind=… · |

### 追踪矩阵

| Requirement | Test | Implementation | Evidence（`kind=` · 路径） | Result |
|---|---|---|---|---|
| R-001 | T-001 | | kind=… · | Pass / Fail / Blocked |

> Pass 禁止仅 `kind=window-smoke` / `health` / 裸 `*smoke*.json`。数据面 success 须 `api-diff` 或 `network-har`。

### Fail / Blocked 分类

| Test | 表象 | 分类 | 根因 / 未知 | 统一 Repair 组 / 外部阻塞 |
|---|---|---|---|---|
| | | implementation / product-ux / plan / test-oracle / environment / new-request / unknown-root-cause | | |

### 统一 Repair 方案

> 所有适用测试完成后才填写。按根因分组，再集中修改。

| 根因组 | 受影响 Tests / 入口 | 修改面 | 回归批次 | 状态 |
|---|---|---|---|---|
| | | | | pending / repaired / blocked |

## 关版

### 结论

- verify-deliver: （宣称可交付 / acceptance-passed / prod-smoke 通过前须 `make verify-deliver`；成功后由脚本写入 `ok · <时间>`）
- 是否可以交付：`可交付 | 不可交付 | 受阻`
- 一句话原因：
- 本次验收覆盖：
- 验收时间：

### 如何体验

- 环境 / 入口：
- 适用角色：
- 代表性数据：
- 成功时应看到：

### 已验证的用户结果

| 用户要完成的事 | 操作 → 两态观察差 | 证据（`kind=`） | 结论 |
|---|---|---|---|
| | | kind=… · | 已通过 / 未通过 / 无法验证 |

> 无两态观察差不得写「已通过」。Build 轨不得填「是否可以交付：可交付」。

### 未通过或无法验证

| 问题 | 对用户的影响 | 严重度 | 原因或缺少什么 | 建议动作 |
|---|---|---|---|---|
| | | 阻断 / 严重 / 一般 / 轻微 | | |

### 限制与上线状态

- 未覆盖内容：
- 已知限制：
- 是否已上线：`未上线 | 已上线 | 未知 | 不适用`
- 上线后的观察：

### 需要用户做什么

- 探活执行者：`agent` | `blocked-needs-auth`
- 需要用户做什么：`无需动作`（默认）| 批准… | 真人SSO/密钥…
- 下一步：（无待决则写「无」；禁止派打开/硬刷发现故障）

## 终态

- Matrix：`matrix-accounted | incomplete`
- Acceptance：`awaiting-human-acceptance | acceptance-passed | not-passed | n/a`
- 结论：`awaiting-human-acceptance | acceptance-passed | blocked | needs-authorization`
- Workspace / Branch / PR：
- 未完成项 / 外部阻塞：
- 直接证据：
- 下一步：

所有适用 Test 有终态只能声明 `matrix-accounted`；工程证伪+人类验收包就绪 → `awaiting-human-acceptance`；
人确认通过且关版条件满足 → `acceptance-passed`。均不是 Delivery Target（见 workflow-contract「状态词汇」）。

## Production Verification（适用时）

> 产品冒烟（目标环境 · P6）态写入后再报生产交付。`/health` 与进程 active 是 P5 过程信号。
> 通则见 `evidence-contract.md` Deliver Gate 与 `verification-loop.md`；阶段见 Skill `deploy`。

- 声明目标 / 实际达到：
- 定级 + 理由（若适用）：
- P2 发布方案（执行序 / sidecar 采纳或延期）：
- P3 验证方案（冒烟层勾选）：
- Deploy / rollback（P5）：
- Health（过程）：
- 环境门禁：
- 产品冒烟（目标环境 · P6）：通过 | 未过 | Blocked+原因
- 探活执行者：`agent` | `blocked-needs-auth`
- 产品冒烟证据：`kind=… · …`
- 需要用户做什么：`无需动作` | …
- 完成标签：`[部署·L#·prod-smoke …]`
- Open MUST（延期项 → 下次 P1）：
- 原始故障信号：
- 核心用户路径：
- 数据一致性：
- 监控观察窗口：
- 回滚点：
