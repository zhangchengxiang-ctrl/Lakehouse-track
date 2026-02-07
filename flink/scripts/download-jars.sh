#!/bin/bash
# Flink 依赖 JAR 下载脚本（Paimon JDBC 模式，无 Hive）
# 优先阿里云镜像，404 时回退 Maven Central
# 运行方式: bash flink/scripts/download-jars.sh

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

# 清理已移除的 JAR（Hive、Hadoop 等）
shopt -s nullglob 2>/dev/null || true
for f in flink-sql-connector-hive-*.jar hive-exec-*.jar mysql-connector-j-*.jar flink-shaded-hadoop-2-uber-*.jar; do
  [ -f "$f" ] && rm -v "$f" && echo "  🗑 已移除 $f"
done

echo "🚀 开始下载 Flink 依赖 JAR 包..."

# ========== 核心：S3 与 Paimon（无 Hadoop，使用 flink-s3-fs-presto） ==========
echo ""
echo "📦 核心（埋点链路 01-03 + PG CDC->Paimon 05）..."
download "org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
# 仅保留 hadoop 实现，避免与 presto 冲突导致 NoSuchMethodError
download "org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar"
download "org/apache/paimon/paimon-flink-1.18/0.9.0/paimon-flink-1.18-0.9.0.jar"
download "org/apache/paimon/paimon-s3/0.9.0/paimon-s3-0.9.0.jar"
download "org/apache/flink/flink-connector-files/1.18.1/flink-connector-files-1.18.1.jar"
download "org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
download "com/ververica/flink-sql-connector-postgres-cdc/3.0.1/flink-sql-connector-postgres-cdc-3.0.1.jar"

echo ""
echo "✅ 所有 JAR 包下载完成！"
ls -lh
