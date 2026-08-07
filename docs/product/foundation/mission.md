# 使命与边界

## 使命

建设以埋点为核心的数据平台：采集 → 数据湖 → 数据仓库，在实验室可落地验证，再演进生产。

## 做

- 埋点采集（Nginx + Vector + MetaAPI/MetaSync，神策协议适配）→ S3 → StarRocks Pipe
- CDC 数据湖（PostgreSQL → Flink CDC → Paimon，Hive Metastore）
- 数据仓库（StarRocks 存算分离、Paimon Catalog、Pipe）
- 元数据与对象存储配套（PostgreSQL、MinIO/S3）
- Docker Compose / K8s 实验室部署（`make install|verify|run-sql`）

## 不做（本期）

- AI 对账门 / LLM 质量闸
- DataSage、Paperclip、agent-kit 及知识卡片等异仓产品
- 面向终端用户的分析产品 UI（BI 由外部工具承担）

## 系统边界

```text
埋点：[神策 SDK] → [collection] → [S3 TSV.gz] → [StarRocks Pipe] → [BI/SQL]
CDC： [PostgreSQL] → [Flink CDC] → [Paimon] → [StarRocks Catalog] → [BI/SQL]
```
