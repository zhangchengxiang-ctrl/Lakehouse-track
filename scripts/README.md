# Lakehouse-track 脚本中心

本项目采用统一的脚本入口 `lakehouse.sh`，简化了所有运维和初始化操作。

## 统一入口
```bash
./scripts/lakehouse.sh <子命令> [参数...]
```

## 子命令详解

| 子命令 | 说明 | 使用场景 |
|--------|------|----------|
| `install` | 一键下载 Flink JAR、GeoIP 数据库、StarRocks JAR | 首次部署前必跑 |
| `run-sql` | 提交 `flink.sql` 任务并创建 StarRocks 外部目录/视图 | 初始化业务逻辑 |
| `fix` | 自动取消 Flink 任务、重建容器并重新提交 SQL | Flink 任务卡死或资源报错时使用 |
| `verify` | 发送测试埋点并打印全链路各环节状态 | 验证数据是否通畅 |
| `reset` | 停止服务、**删除 data/ 目录所有数据**、重新启动 | 环境彻底搞乱后的一键重置 |
| `download-starrocks-jars` | 仅下载 StarRocks 外部目录所需的 Paimon 相关 JAR | 仅更新 JAR 时使用 |

## 环境变量
你可以通过环境变量覆盖默认配置：
- `STARROCKS_HOST`: 默认 `127.0.0.1`
- `STARROCKS_PORT`: 默认 `9030`
- `STARROCKS_USER`: 默认 `root`

## 示例
```bash
# 验证全链路
./scripts/lakehouse.sh verify

# 仅重新执行 StarRocks SQL
./scripts/lakehouse.sh run-sql starrocks

# 修复 Flink 任务
./scripts/lakehouse.sh fix
```
