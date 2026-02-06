# Mac Docker 环境 K8s 运行手册 (生产级 Lakehouse 架构)

本手册详细说明如何在 **Mac + Docker Desktop** 环境下运行基于 **神策数据 (Sensors Data) SDK** 适配的埋点全链路 K8s 集群。

---

## 一、 前置要求

### 1.1 硬件分配 (关键)
由于 StarRocks 和 Flink 内存需求较高，请确保 Docker Desktop 资源分配如下：
- **Memory**: 至少 **12GB** (建议 16GB+)
- **CPU**: 至少 **4 核**
- **Disk**: 至少 **60GB**

### 1.2 软件依赖
- **Docker Desktop**: 4.0+ (需启用 Kubernetes)
- **kubectl**: 1.24+
- **mysql-client**: 用于连接 StarRocks (`brew install mysql-client`)

---

## 二、 部署步骤

### 2.1 准备 Flink 依赖 JAR
在启动集群前，需将以下 JAR 包放入项目根目录的 `flink-lib/` 文件夹中。

**快速下载 (推荐)**:
```bash
bash scripts/download-jars.sh
```

**手动下载清单**:
- `flink-sql-connector-files-1.18.0.jar`
- `flink-sql-connector-postgres-cdc-2.5.0.jar`
- `flink-connector-starrocks-1.2.9_flink-1.18.jar`
- `paimon-flink-1.18-1.0.0.jar`
- `flink-s3-fs-hadoop-1.18.0.jar`
- `postgresql-42.7.3.jar`

> 详细下载链接请参考 `flink-lib/README.md`。

### 2.2 构建采集端镜像
```bash
cd /Users/leo/code/track
docker build -t paimon-lab/nginx-vector:latest ./nginx-vector
```

### 2.3 一键部署 K8s 资源
```bash
kubectl apply -k k8s/
```

### 2.4 等待服务就绪
```bash
kubectl get pods -n paimon-lab -w
```
等待所有 Pod 状态变为 `Running`，且 `init-minio-bucket` Job 状态为 `Completed`。

---

## 三、 全链路配置 (SQL 实施)

### 3.1 Flink 入湖配置
进入 Flink SQL Client：
```bash
kubectl exec -it -n paimon-lab deployment/flink-jobmanager -- ./bin/sql-client.sh
```
按顺序执行以下脚本内容（文件路径：`flink-sql/`）：
1.  **创建 Catalog**: 执行 `01-catalog.sql`
2.  **创建 Paimon 表**: 执行 `02-paimon-tables.sql`
3.  **启动入湖任务**: 执行 `03-ingestion.sql` (建议分别提交 Job 1/2/3)

### 3.2 StarRocks 分析配置
连接 StarRocks (默认端口 9030)：
```bash
mysql -h 127.0.0.1 -P 9030 -u root
```
执行以下脚本内容（文件路径：`starrocks-sql/`）：
1.  **挂载 Paimon 湖**: 执行 `01-external-catalog.sql`
2.  **创建视图与物化视图**: 执行 `02-views-mvs.sql`

---

## 四、 验证与测试

### 4.1 发送模拟神策埋点
神策 SDK 发送的数据经过 Base64 编码。使用以下命令模拟一次“查看商品”事件：

```bash
# 原始数据: {"distinct_id":"888","event":"ViewProduct","type":"track","properties":{"price":100},"time":1707180000000}
curl -X POST "http://localhost/sa?data=eyJkaXN0aW5jdF9pZCI6Ijg4OCIsImV2ZW50IjoiVmlld1Byb2R1Y3QiLCJ0eXBlIjoidHJhY2siLCJwcm9wZXJ0Ijp7InByaWNlIjoxMDB9LCJ0aW1lIjoxNzA3MTgwMDAwMDAwfQ=="
```

### 4.2 检查数据流向
1.  **Vector 日志**: `kubectl logs -f deployment/nginx-vector -n paimon-lab` (确认 Base64 已解码)
2.  **MinIO 存储**: 访问 `http://localhost:9001` (minioadmin/minioadmin)，检查 `paimon-lake/staging/` 是否生成 Parquet 文件。
3.  **StarRocks 查询**:
    ```sql
    -- 检查原始 ODS 数据
    SELECT * FROM ods.ods_events LIMIT 10;
    -- 检查物化视图刷新状态
    SHOW MATERIALIZED VIEWS;
    ```

---

## 五、 生产运维进阶

### 5.1 提交独立 Compaction 任务
在生产环境下，为了保证写入性能，需启动独立的合并任务：
```bash
kubectl exec -it -n paimon-lab deployment/flink-jobmanager -- \
  ./bin/flink run -c org.apache.paimon.flink.action.CompactAction /opt/flink/lib/paimon-flink-*.jar \
  --warehouse s3://paimon-lake/data/ \
  --database default \
  --table ods_events_core
```

### 5.2 常用运维命令
- **查看 Flink Web UI**: `http://localhost:8081`
- **重启采集端**: `kubectl rollout restart deployment/nginx-vector -n paimon-lab`
- **清理环境**: `kubectl delete namespace paimon-lab`

---

*文档版本：2026-02 (神策适配版)*
