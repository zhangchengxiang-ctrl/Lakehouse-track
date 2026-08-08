# 差距账（Gap Register）

> 对照：`docs/architecture/完整方案-可落地版.md`（目标态）vs 仓库运行时。

| ID | 能力 | 蓝图 | 现状 | 严重度 | 状态 |
|----|------|------|------|--------|------|
| GAP-001 | C-001 采集→湖→仓 | `foundation/mission.md` | Compose 双链路可验；缺 CI | P2 | open |
| GAP-002 | 可观测 | 架构 §9 黄金指标 | 无统一监控 / 告警 | P1 | open |
| GAP-003 | StarRocks FE HA | 架构 §5 FE≥3 | Compose/K8s 均为 FE×1 | P0 | open |
| GAP-004 | Vector 缓冲持久化 | 架构 ADR-6 PVC | K8s `emptyDir`；Compose anonymous volume | P0 | open |
| GAP-005 | 对象存储生产形态 | 托管多 AZ S3/OSS | 实验室 MinIO（Compose 单机 / K8s 4 节点） | P0 | open |
| GAP-006 | PostgreSQL HA | 多 AZ + 备份/PITR | 单实例 StatefulSet / Compose | P0 | open |
| GAP-007 | CN DataCache 校准 | limit = 盘 70–80% | `cn.conf` `starlet_cache_limit_gb=10` vs K8s 200Gi PVC | P1 | open |
| GAP-008 | 采集 HPA 成本基线 | minReplicas 跨 AZ 2~3 | K8s HPA `minReplicas: 6` | P2 | open |
| GAP-009 | CDC 生产编排 | Flink JM HA + checkpoint on S3；进 K8s | 仅 Compose；`deploy/k8s` 无 Flink/HMS | P1 | open |
| GAP-010 | S3 Lifecycle / 分级保留 | event_group + 热冷分层 | 仓内动态分区有设计；对象层 Lifecycle 未落地 | P2 | open |
| GAP-011 | Meta/PG 降级策略 | 架构 ADR-9 fail-open | 已拍板：采集放行；注册可降级 | — | closed |
| GAP-012 | 压测与故障演练 | 架构 §6.3 / §9.2 | 仅有 `make bench` 实验室入口 | P1 | open |
| GAP-013 | 架构文档对齐 | 双链路 + SoT | 2026-08 已重写设计稿 | — | closed |
| GAP-014 | 消息队列选型 | ADR-7 默认不上 Kafka | 已成文；主路径不插 MQ | — | closed |
| GAP-015 | 采集解析栈 | ADR-8 继续 Vector/VRL | 已成文；换语言须满足触发条件 | — | closed |
