#!/bin/bash
# Flink 依赖 JAR 下载脚本（Paimon JDBC 模式，无 Hive）
# 运行方式: bash scripts/download-jars.sh

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_DIR=$(dirname "$SCRIPT_DIR")
LIB_DIR="$PROJECT_DIR/flink_lib"
mkdir -p "$LIB_DIR"
cd "$LIB_DIR"

echo "🚀 开始下载 Flink 依赖 JAR 包（Paimon JDBC 模式）..."

# ========== S3 与 Paimon ==========
echo ""
echo "📦 S3 与 Paimon 相关..."

curl -sSfLO https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-presto/1.18.1/flink-s3-fs-presto-1.18.1.jar
curl -sSfLO https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
curl -sSfLO https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.18/0.9.0/paimon-flink-1.18-0.9.0.jar
curl -sSfLO https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/0.9.0/paimon-s3-0.9.0.jar

# ========== 埋点链路专用 ==========
echo ""
echo "📦 埋点链路专用..."

# Filesystem Connector（读取 S3 staging Parquet）
curl -sSfLO https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-files/1.18.0/flink-sql-connector-files-1.18.0.jar

# PostgreSQL JDBC（Paimon JDBC Catalog）
curl -sSfLO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# StarRocks Connector（CDC 写入）
curl -sSfLO https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.9_flink-1.18/flink-connector-starrocks-1.2.9_flink-1.18.jar

# Postgres CDC
curl -sSfLO https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.5.0/flink-sql-connector-postgres-cdc-2.5.0.jar

echo ""
echo "✅ 所有 JAR 包下载完成！"
ls -lh
