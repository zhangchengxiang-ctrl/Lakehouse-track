# Flink 依赖 JAR

将以下 JAR 放入此目录，Docker Compose 会挂载到 Flink 的 `/opt/flink/lib`。

## 快速下载方式 (推荐)

在项目根目录下运行脚本：
```bash
bash scripts/download-jars.sh
```

## 手动下载链接

| 组件 | 版本 | 下载地址 |
|------|------|----------|
| Flink SQL Files | 1.18.0 | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-files/1.18.0/flink-sql-connector-files-1.18.0.jar) |
| Postgres CDC | 2.5.0 | [Download](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.5.0/flink-sql-connector-postgres-cdc-2.5.0.jar) |
| StarRocks Connector | 1.2.9 | [Download](https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.9_flink-1.18/flink-connector-starrocks-1.2.9_flink-1.18.jar) |
| Paimon Flink | 1.0.0 | [Download](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.18/1.0.0/paimon-flink-1.18-1.0.0.jar) |
| S3 FS Hadoop | 1.18.0 | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.0/flink-s3-fs-hadoop-1.18.0.jar) |
| PostgreSQL Driver | 42.7.3 | [Download](https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar) |

