# StarRocks 目录

| 文件/目录 | 说明 |
|----------|------|
| `config/` | FE 共享配置（存算分离 S3） |
| `starrocks.sql` | 外部目录、视图、物化视图（合并脚本） |
| `jars/` | Paimon External Catalog 依赖（lakehouse.sh install 下载） |

```bash
./scripts/lakehouse.sh run-sql
```
