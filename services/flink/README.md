# Flink 目录

| 子目录/文件 | 说明 |
|-------------|------|
| `config/` | Flink 集群配置 |
| `lib/` | 依赖 JAR（`scripts/download-jars.sh` 下载，已 gitignore） |
| `flink.sql` | CDC 入湖脚本（Catalog + 源表 + 目标表 + INSERT） |
| `scripts/` | JAR 下载等工具脚本 |

## 执行方式

```bash
make install            # 下载依赖 JAR
make run-sql ARGS=flink # 提交 Flink CDC 作业
```
