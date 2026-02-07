# SQL 脚本

按执行环境分类：

| 目录 | 执行方式 |
|------|----------|
| `flink/` | `docker compose exec flink-jobmanager ./bin/sql-client.sh` 中按顺序执行 |
| `starrocks/` | `mysql -h 127.0.0.1 -P 9030 -u root` 中按顺序执行 |

## Flink 执行顺序
1. `01-catalog.sql` — Paimon JDBC Catalog
2. `02-paimon-tables.sql` — 创建表
3. `03-ingestion.sql` — 入湖任务
4. `04-cdc-orders.sql` — CDC 示例（可选）

## StarRocks 执行顺序
1. `01-external-catalog.sql` — 挂载 Paimon
2. `02-views-mvs.sql` — 视图与物化视图
