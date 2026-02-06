#!/bin/bash
# 自动下载 Flink 依赖 JAR 包脚本
# 运行方式: cd track/flink-lib && bash ../scripts/download-jars.sh

set -e

LIB_DIR=$(dirname "$0")/../flink-lib
mkdir -p "$LIB_DIR"
cd "$LIB_DIR"

echo "🚀 开始下载 Flink 依赖 JAR 包..."

# 1. Flink SQL Connector Files
echo "📦 Downloading Flink SQL Connector Files..."
curl -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-files/1.18.0/flink-sql-connector-files-1.18.0.jar

# 2. Flink SQL Connector Postgres CDC
echo "📦 Downloading Postgres CDC..."
curl -O https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.5.0/flink-sql-connector-postgres-cdc-2.5.0.jar

# 3. Flink Connector StarRocks (1.2.9 for Flink 1.18)
echo "📦 Downloading StarRocks Connector..."
curl -O https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.9_flink-1.18/flink-connector-starrocks-1.2.9_flink-1.18.jar

# 4. Paimon Flink 1.18 (1.0.0 is the stable version for Flink 1.18)
echo "📦 Downloading Paimon Flink..."
curl -O https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.18/1.0.0/paimon-flink-1.18-1.0.0.jar

# 5. Flink S3 FS Hadoop
echo "📦 Downloading S3 FS Hadoop..."
curl -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.0/flink-s3-fs-hadoop-1.18.0.jar

# 6. PostgreSQL JDBC Driver
echo "📦 Downloading PostgreSQL Driver..."
curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

echo "✅ 所有 JAR 包下载完成！"
ls -lh
