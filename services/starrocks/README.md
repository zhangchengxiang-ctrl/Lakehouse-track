# StarRocks 目录

| 文件/目录 | 说明 |
|----------|------|
| `config/` | FE 共享配置（存算分离 S3） |
| `starrocks.sql` | 外部目录、视图、物化视图（合并脚本） |
| `starrocks-pipes.sql` | Pipe 入仓 |
| `jars/` | Paimon External Catalog 依赖（`make install` 下载） |

## 两种本地模式（互斥，同抢 9030）

| 模式 | 启动 | 用途 |
|------|------|------|
| **存算分离（本仓目标态）** | `docker compose up -d starrocks-fe starrocks-cn` | Paimon Catalog / Pipe / CN 弹性 |
| **all-in-one（收编自 flowgpt）** | `docker compose --profile allin1 up -d starrocks` | 保留原 `flowgpt-starrocks` 数据卷，供 DataSage / SQLMesh |

数据卷：外部卷 `flowgpt-data-platform_starrocks_data`（不随 track `down -v` 删除）。

```bash
./scripts/lakehouse.sh run-sql ARGS=starrocks
# 自动优先 starrocks-fe，否则走 flowgpt-starrocks
```
