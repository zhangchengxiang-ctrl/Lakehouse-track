# Flink 目录

Flink 相关内容统一存放于此。

| 子目录 | 说明 |
|--------|------|
| `config/` | Flink 集群配置 |
| `lib/` | 依赖 JAR（`scripts/download-jars.sh` 下载） |
| `dist/` | StreamPark 挂载的 Flink 发行版（运行时生成） |
| `flink.sql` | 合并脚本（Catalog、表、入湖、CDC） |
| `scripts/` | 下载 JAR 等工具脚本 |

## 执行顺序

1. `bash flink/scripts/download-jars.sh`
2. `docker compose up -d --build`
3. `docker compose exec flink-jobmanager ./bin/sql-client.sh`
4. 执行 `docker compose exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/flink.sql`
