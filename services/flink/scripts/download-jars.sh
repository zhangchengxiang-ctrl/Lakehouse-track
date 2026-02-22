#!/bin/bash
# Flink 依赖 JAR 下载脚本（Paimon Hive Metastore）
# 优先阿里云镜像，404 时回退 Maven Central
# 运行方式: bash services/flink/scripts/download-jars.sh

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
FLINK_DIR=$(dirname "$SCRIPT_DIR")
LIB_DIR="$FLINK_DIR/lib"
ALIYUN="https://maven.aliyun.com/repository/central"
MAVEN="https://repo1.maven.org/maven2"

download() {
  local path=$1
  local file=$(basename "$path")
  if [ -f "$file" ]; then
    echo "  ✓ $file (已存在，跳过)"
    return 0
  fi
  if curl -sSfL -o "$file" "$ALIYUN/$path" 2>/dev/null; then
    echo "  ✓ $file (阿里云)"
  else
    echo "  ✓ $file (Maven Central)"
    curl -sSfLO "$MAVEN/$path"
  fi
}

mkdir -p "$LIB_DIR"
cd "$LIB_DIR"

# 清理旧版本 JAR（避免 Hive/Paimon 版本冲突）
shopt -s nullglob 2>/dev/null || true
for f in flink-sql-connector-hive-*.jar hive-*.jar mysql-connector-j-*.jar flink-shaded-hadoop-2-uber-*.jar; do
  [ -f "$f" ] && rm -v "$f" && echo "  🗑 已移除 $f"
done

# 清理历史 Paimon JAR（避免 0.8/0.9 混用导致 classpath 冲突）
for f in paimon-*.jar; do
  [ -f "$f" ] && rm -v "$f" && echo "  🗑 已移除 $f"
done

echo "🚀 开始下载 Flink 依赖 JAR 包..."

# ========== 核心：S3 与 Paimon ==========
echo ""
echo "📦 核心（埋点链路 01-03 + PG CDC->Paimon 05）..."
download "org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
# 仅保留 hadoop 实现，避免与 presto 冲突导致 NoSuchMethodError
download "org/apache/flink/flink-s3-fs-hadoop/1.20.3/flink-s3-fs-hadoop-1.20.3.jar"
# Paimon 采用 bundled jar（减少 classpath 冲突）
download "org/apache/paimon/paimon-flink-1.20/1.3.1/paimon-flink-1.20-1.3.1.jar"
download "org/apache/flink/flink-connector-files/1.20.3/flink-connector-files-1.20.3.jar"
download "org/apache/flink/flink-connector-jdbc/1.20.3/flink-connector-jdbc-1.20.3.jar"
download "org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
# Flink CDC 3.5.0（Flink 1.20.x 生态最稳）
download "org/apache/flink/flink-sql-connector-postgres-cdc/3.5.0/flink-sql-connector-postgres-cdc-3.5.0.jar"

echo ""
echo "📦 Hive Metastore（Paimon HMS Catalog）..."
# Flink Hive connector（匹配 Flink 1.20.3 + Hive 3.1.3）
download "org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.20.3/flink-sql-connector-hive-3.1.3_2.12-1.20.3.jar"
# Hive client 依赖（最小可用集）
download "org/apache/hive/hive-common/3.1.3/hive-common-3.1.3.jar"
download "org/apache/hive/hive-metastore/3.1.3/hive-metastore-3.1.3.jar"
download "org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar"
download "io/dropwizard/metrics/metrics-core/3.2.6/metrics-core-3.2.6.jar"
download "org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar"
download "org/datanucleus/datanucleus-api-jdo/4.2.4/datanucleus-api-jdo-4.2.4.jar"
download "org/datanucleus/datanucleus-core/4.1.17/datanucleus-core-4.1.17.jar"
download "org/datanucleus/datanucleus-rdbms/4.1.19/datanucleus-rdbms-4.1.19.jar"
download "javax/jdo/jdo-api/3.0.1/jdo-api-3.0.1.jar"

echo ""
echo "✅ 所有 JAR 包下载完成！"
ls -lh

echo ""
echo "📦 Hive Metastore S3 依赖（独立目录）..."
HMS_LIB_DIR="$FLINK_DIR/../hive/lib"
mkdir -p "$HMS_LIB_DIR"
cd "$HMS_LIB_DIR"
download "org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar"
download "com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar"
ls -lh
