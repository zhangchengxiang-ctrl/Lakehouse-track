#!/usr/bin/env bash
# Lakehouse-track 统一脚本入口
# 用法：./scripts/lakehouse.sh <子命令> [参数...]
#
# 子命令：
#   install        安装所有依赖（Flink JAR、GeoIP、StarRocks JAR、配置校验）
#   fix            修复 Flink 入湖（取消任务、重启、重新执行 flink.sql）
#   verify         验证埋点数据链路
#   replay         重放 test_data 中的神策日志
#   sync-metadata  转换并导入埋点元数据（MySQL -> PG）
#   reset          清除数据并重建（含 flink.sql、starrocks.sql）
#   run-sql [flink|starrocks|文件] 执行 SQL（无参数时执行 flink.sql + starrocks.sql）
#   download-starrocks-jars  仅下载 StarRocks 外部目录依赖
#
# 示例：
#   ./scripts/lakehouse.sh install
#   ./scripts/lakehouse.sh fix
#   ./scripts/lakehouse.sh run-sql

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_DIR=$(dirname "$SCRIPT_DIR")
DATA_DIR="$PROJECT_DIR/data"

# StarRocks 连接配置
STAR_HOST="${STARROCKS_HOST:-127.0.0.1}"
STAR_PORT="${STARROCKS_PORT:-9030}"
STAR_USER="${STARROCKS_USER:-root}"
STAR_PASS="${STARROCKS_PASS:-}"

# -----------------------------------------------------------------------------
# 辅助函数
# -----------------------------------------------------------------------------

_run_mysql() {
  if docker compose ps starrocks-fe 2>/dev/null | grep -q "Up"; then
    docker compose exec -T starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root "$@"
  elif command -v mysql &>/dev/null; then
    mysql -h "$STAR_HOST" -P "$STAR_PORT" -u "$STAR_USER" ${STAR_PASS:+-p"$STAR_PASS"} "$@"
  else
    echo "需要 mysql 客户端或运行中的 starrocks-fe 容器"
    exit 1
  fi
}

run_flink_sql() {
  local f=$1
  [ -z "$f" ] && f="flink/flink.sql"
  echo ">>> $f (Flink)"

  # 终极修复：使用“交互模式 stdin”一次性执行整份脚本（单 Session）
  # 背景：Flink 1.18.1 的 sql-client 在 -f 模式下可能触发 only single statement supported，
  # 但交互模式可以逐条解析并保持会话态（Catalog/临时表均有效）。
  docker compose exec -T -e HMS_URI="${HMS_URI:-thrift://hive-metastore:9083}" \
    flink-jobmanager bash -c \
    "cp /opt/flink/flink.sql /tmp/run.sql && \
     HMS_URI=\"\${HMS_URI:-thrift://hive-metastore:9083}\" && \
     HMS_HOST=\$(echo \"\$HMS_URI\" | sed -E 's#^thrift://([^:/]+).*#\\1#') && \
     HMS_PORT=\$(echo \"\$HMS_URI\" | sed -E 's#^thrift://[^:/]+:([0-9]+).*#\\1#') && \
     echo \"  等待 HMS (\$HMS_HOST:\$HMS_PORT)...\" && \
     for i in \$(seq 1 30); do \
       (echo > /dev/tcp/\$HMS_HOST/\$HMS_PORT) >/dev/null 2>&1 && break; \
       sleep 2; \
     done && \
     sed -i \"s|thrift://hive-metastore:9083|\${HMS_URI}|g\" /tmp/run.sql && \
     echo 'QUIT;' >> /tmp/run.sql && \
     ./bin/sql-client.sh < /tmp/run.sql"
}

run_starrocks_sql() {
  local f=$1
  [ -z "$f" ] && f="starrocks/starrocks.sql"
  echo ">>> $f (StarRocks)"
  
  # 优化：幂等清理。StarRocks 3.x 某些对象不支持 IF EXISTS，通过 || true 忽略不存在的错误
  _run_mysql -e "DROP MATERIALIZED VIEW IF EXISTS ods.dws_daily_active_users" 2>/dev/null || true
  _run_mysql -e "DROP MATERIALIZED VIEW IF EXISTS ods.dwd_user_full_track" 2>/dev/null || true
  _run_mysql -e "DROP RESOURCE GROUP mv_refresh_group" 2>/dev/null || true
  _run_mysql -e "DROP DATABASE IF EXISTS ods" 2>/dev/null || true
  _run_mysql -e "DROP CATALOG paimon_catalog" 2>/dev/null || true
  
  echo "  创建 Paimon Catalog..."
  _run_mysql -e "CREATE EXTERNAL CATALOG paimon_catalog PROPERTIES (\
    \"type\" = \"paimon\",\
    \"paimon.catalog.type\" = \"hive\",\
    \"hive.metastore.uris\" = \"thrift://hive-metastore:9083\",\
    \"paimon.catalog.warehouse\" = \"s3a://paimon-lake/paimon_data\",\
    \"aws.s3.endpoint\" = \"http://minio:9000\",\
    \"aws.s3.access_key\" = \"minioadmin\",\
    \"aws.s3.secret_key\" = \"minioadmin\",\
    \"aws.s3.enable_ssl\" = \"false\",\
    \"aws.s3.enable_path_style_access\" = \"true\"\
  )" 2>/dev/null || true

  echo "  等待 Paimon 元数据就绪..."
  for i in $(seq 1 20); do
    dbs=$(_run_mysql -N -e "SHOW DATABASES FROM paimon_catalog" 2>/dev/null || true)
    if python - <<'PY' "$dbs"
import sys
dbs = sys.argv[1].split()
sys.exit(0 if "ods" in dbs else 1)
PY
    then
      echo "  ✓ 发现 Paimon 数据库 ods"
      break
    fi
    echo "  等待 ods... ($i/20)"
    sleep 3
  done

  echo "  等待元数据清理..."
  sleep 2
  _run_mysql < "$f"
}

# -----------------------------------------------------------------------------
# 子命令实现
# -----------------------------------------------------------------------------

cmd_install() {
  echo "=========================================="
  echo "  Lakehouse-track 依赖安装"
  echo "=========================================="

  echo ""
  echo "[1/4] Flink 依赖 JAR..."
  bash "$PROJECT_DIR/flink/scripts/download-jars.sh"

  echo ""
  echo "[2/4] GeoIP 数据库..."
  GEOIP_DIR="$PROJECT_DIR/vector/geoip"
  GEOIP_FILE="$GEOIP_DIR/GeoLite2-City.mmdb"
  mkdir -p "$GEOIP_DIR"

  if [ -f "$GEOIP_FILE" ]; then
    echo "  ✓ GeoLite2-City.mmdb 已存在"
  else
    if [ -f "$PROJECT_DIR/scripts/deps.env" ]; then
      # shellcheck source=/dev/null
      . "$PROJECT_DIR/scripts/deps.env"
    fi
    GEOIP_URL="${GEOIP_URL:-https://ghproxy.com/https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb}"
    echo "  下载 GeoLite2-City.mmdb ..."
    if curl -sSfL --connect-timeout 30 -o "$GEOIP_FILE" "$GEOIP_URL" 2>/dev/null; then
      echo "  ✓ GeoLite2-City.mmdb 下载完成"
    else
      if curl -sSfL --connect-timeout 30 -o "$GEOIP_FILE" "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb" 2>/dev/null; then
        echo "  ✓ GeoLite2-City.mmdb 下载完成"
      else
        echo "  ⚠ GeoIP 下载失败，请手动下载到 vector/geoip/GeoLite2-City.mmdb"
        echo "    参见 vector/geoip/README.md"
      fi
    fi
  fi

  echo ""
  echo "[3/4] StarRocks 外部目录依赖 JAR..."
  cmd_download_starrocks_jars

  echo ""
  echo "[4/4] 配置校验..."
  if [ -f "$PROJECT_DIR/starrocks/config/fe-shared.conf" ]; then
    echo "  ✓ starrocks/config/fe-shared.conf 存在"
  else
    echo "  ✗ 缺少 starrocks/config/fe-shared.conf"
    exit 1
  fi

  echo ""
  echo "=========================================="
  echo "  ✅ 依赖安装完成"
  echo "=========================================="
  echo ""
  echo "下一步："
  echo "  1. docker compose up -d --build"
  echo "  2. 验证数据链路：./scripts/lakehouse.sh verify"
  echo ""
}

cmd_download_starrocks_jars() {
  JAR_DIR="$PROJECT_DIR/starrocks/jars"
  ALIYUN="https://maven.aliyun.com/repository/central"
  MAVEN="https://repo1.maven.org/maven2"

  download() {
    local path=$1
    local file
    file=$(basename "$path")
    if [ -f "$JAR_DIR/$file" ]; then
      echo "  ✓ $file (已存在，跳过)"
      return 0
    fi
    if curl -sSfL -o "$JAR_DIR/$file" "$ALIYUN/$path" 2>/dev/null; then
      echo "  ✓ $file (阿里云)"
    else
      echo "  ✓ $file (Maven Central)"
      curl -sSfL -o "$JAR_DIR/$file" "$MAVEN/$path"
    fi
  }

  mkdir -p "$JAR_DIR"
  # 清理历史 Paimon JAR（避免 0.8/0.9 混用导致 classpath 冲突）
  rm -f "$JAR_DIR"/paimon-bundle-*.jar "$JAR_DIR"/paimon-s3-*.jar 2>/dev/null || true
  download "org/apache/paimon/paimon-bundle/1.3.1/paimon-bundle-1.3.1.jar"
  download "org/apache/paimon/paimon-s3/1.3.1/paimon-s3-1.3.1.jar"
  echo ""
  echo "✅ StarRocks 依赖 JAR 包下载完成！"
  ls -lh "$JAR_DIR"
}

cmd_fix() {
  echo "=== 1. 下载 Flink 依赖（含 flink-s3-fs-hadoop）==="
  bash "$PROJECT_DIR/flink/scripts/download-jars.sh"

  echo ""
  echo "=== 2. 取消所有 Flink 任务 ==="
  for id in $(curl -s "http://localhost:8081/v1/jobs" 2>/dev/null | python3 -c "
import json,sys
for j in json.load(sys.stdin).get('jobs',[]):
    print(j['id'])
" 2>/dev/null); do
  echo "  取消 $id"
  curl -s -X PATCH "http://localhost:8081/v1/jobs/$id?mode=cancel" 2>/dev/null || true
done
  sleep 2

  echo ""
  echo "=== 3. 重建并重启 Flink ==="
  cd "$PROJECT_DIR"
  docker compose build flink-jobmanager flink-taskmanager
  docker compose up -d flink-jobmanager flink-taskmanager

  echo ""
  echo "=== 4. 等待 Flink 就绪 ==="
  for i in $(seq 1 20); do
    if curl -s "http://localhost:8081/v1/taskmanagers" 2>/dev/null | grep -q "taskmanagers"; then
      echo "  Flink 已就绪 (TaskManager 已注册)"
      sleep 5
      break
    fi
    echo "  等待 Flink... ($i/20)"
    sleep 5
  done

  echo ""
  echo "=== 5. 执行 flink.sql ==="
  run_flink_sql

  echo ""
  echo "=== 完成 ==="
  echo "  约 30–60 秒后可在 StarRocks 查询："
  echo "  SELECT * FROM paimon_catalog.\`default\`.ods_events_core;"
  echo "  或再次运行 curl 发送测试数据后等待"
}

cmd_verify() {
  cd "$PROJECT_DIR"

  echo "=== 0. 发送测试埋点 (重放历史日志) ==="
  if [ -f "$SCRIPT_DIR/replay_logs.py" ]; then
    python3 "$SCRIPT_DIR/replay_logs.py"
  else
    # 备用方案：发送单条测试埋点
    TEST_URL="http://localhost/sa?data=eyJkaXN0aW5jdF9pZCI6Ijg4OCIsImV2ZW50IjoiVmlld1Byb2R1Y3QiLCJ0eXBlIjoidHJhY2siLCJwcm9wZXJ0Ijp7InByaWNlIjoxMDB9LCJ0aW1lIjoxNzA3MTgwMDAwMDAwfQ=="
    curl -sSf --connect-timeout 5 -o /dev/null "$TEST_URL" 2>/dev/null || echo "  ✗ 请求失败"
  fi
  
  echo "  等待 45 秒让数据流经 Nginx→Vector→MinIO→Flink→Paimon..."
  sleep 45
  echo ""

  echo "=== 1. Nginx 日志（最近 3 条 sa 请求）==="
  grep -o '"sa_data":"[^"]*"' data/nginx_logs/access.log 2>/dev/null | tail -3 || echo "无"

  echo ""
  echo "=== 2. MinIO staging 文件（Vector 输出）==="
  find data/minio/paimon-lake/staging \( -name "*.log" -o -name "*.json" \) 2>/dev/null | head -10 || \
    find data/minio/paimon-lake/staging -type f 2>/dev/null | head -10
  echo "（若为空则 Vector 未写入或路径不同）"

  echo ""
  echo "=== 3. Paimon ods_events_core 数据目录 ==="
  ls -la data/minio/paimon-lake/data/default.db/ods_events_core/ 2>/dev/null || echo "无"
  find data/minio/paimon-lake/data/default.db/ods_events_core -type d -name "bucket-*" 2>/dev/null | head -5 || echo "无 bucket 目录 = 无数据"

  echo ""
  echo "=== 4. Flink 任务状态 ==="
  curl -s "http://localhost:8081/v1/jobs" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for j in d.get('jobs',[]):
    print(f\"  {j['id'][:8]}... {j['status']}\")
" 2>/dev/null || echo "Flink 未启动或不可达"

  echo ""
  echo "=== 5. 建议操作 ==="
  echo "  - 若 MinIO 无 staging 文件：检查 Vector 日志 docker compose logs vector"
  echo "  - 若 Paimon 无 bucket：Flink 任务可能失败，检查 docker compose logs flink-taskmanager"
  echo "  - 若 Flink 任务 RESTARTING：重新提交 flink.sql 或查看 Web UI http://localhost:8081"
  echo "  - 发送测试埋点：$0 verify"
}

cmd_sync_metadata() {
  echo "=== 1. 生成 PG 元数据脚本 ==="
  python3 "$PROJECT_DIR/scripts/convert_user_track_mysql_to_pg.py"

  echo ""
  echo "=== 2. 导入到 PostgreSQL ==="
  if docker compose ps postgres 2>/dev/null | grep -q "Up"; then
    docker compose exec -T postgres psql -U paimon -d paimon_db -f /docker-entrypoint-initdb.d/04-init-user-track.sql
  else
    echo "PostgreSQL 容器未运行，跳过导入"
  fi
}

cmd_reset() {
  echo "=========================================="
  echo "  Lakehouse-track 重置并重建"
  echo "=========================================="

  echo ""
  echo "[1/6] 停止服务..."
  cd "$PROJECT_DIR"
  docker compose down -v 2>/dev/null || true

  echo ""
  echo "[2/6] 清除数据目录..."
  rm -rf "$DATA_DIR/minio"
  rm -rf "$DATA_DIR/postgres"
  rm -rf "$DATA_DIR/starrocks"
  rm -rf "$DATA_DIR/nginx_logs"
  mkdir -p "$DATA_DIR/minio" "$DATA_DIR/postgres" "$DATA_DIR/starrocks/fe" "$DATA_DIR/starrocks/cn" "$DATA_DIR/nginx_logs"

  echo ""
  echo "[3/6] 构建并启动..."
  docker compose up -d --build

  echo ""
  echo "[4/6] 等待服务就绪..."
  sleep 30
  echo "  等待 PostgreSQL healthy..."
  for i in $(seq 1 30); do
    docker compose exec -T postgres pg_isready -U paimon 2>/dev/null && break
    sleep 5
  done
  echo "  初始化 Postgres 元数据库（metastore/streampark）..."
  if ! docker compose exec -T postgres psql -U paimon -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = 'metastore'" 2>/dev/null | grep -q 1; then
    docker compose exec -T postgres psql -U paimon -d postgres -c "CREATE DATABASE metastore" 2>/dev/null || true
  fi
  docker compose exec -T postgres psql -U paimon -d postgres -c \
    "GRANT ALL PRIVILEGES ON DATABASE metastore TO paimon" 2>/dev/null || true
  if ! docker compose exec -T postgres psql -U paimon -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = 'streampark'" 2>/dev/null | grep -q 1; then
    docker compose exec -T postgres psql -U paimon -d postgres -c "CREATE DATABASE streampark" 2>/dev/null || true
  fi
  docker compose exec -T postgres psql -U paimon -d postgres -c \
    "GRANT ALL PRIVILEGES ON DATABASE streampark TO paimon" 2>/dev/null || true
  echo "  等待 MinIO..."
  sleep 10
  # 本项目使用 Hive Metastore，等待 9083 端口就绪，避免 hive CLI 偶发阻塞
  echo "  等待 Hive Metastore..."
  for i in $(seq 1 30); do
    if command -v nc >/dev/null 2>&1; then
      nc -z -w 2 127.0.0.1 9083 && break
    else
      docker compose exec -T hive-metastore bash -c \
        "command -v nc >/dev/null 2>&1 && nc -z -w 2 127.0.0.1 9083" 2>/dev/null && break
    fi
    sleep 5
  done
  echo "  MinIO Bucket 已由 minio-init 自动初始化"

  echo ""
  echo "[5/6] 等待 Flink 就绪并执行 flink.sql..."
  for i in $(seq 1 24); do
    if curl -s "http://localhost:8081/v1/jobmanager/metrics" 2>/dev/null | grep -q "Status"; then
      echo "  Flink 已就绪"
      run_flink_sql
      break
    fi
    echo "  等待 Flink... ($i/24)"
    sleep 5
  done

  echo ""
  echo "[6/6] 等待 StarRocks FE 就绪并执行 starrocks.sql..."
  for i in $(seq 1 30); do
    if _run_mysql -e "SELECT 1" 2>/dev/null; then
      echo "  StarRocks 已就绪"
      run_starrocks_sql
      break
    fi
    echo "  等待 StarRocks... ($i/30)"
    sleep 5
  done

  echo ""
  echo "=========================================="
  echo "  ✅ 重置完成"
  echo "=========================================="
  echo ""
}

cmd_run_sql() {
  cd "$PROJECT_DIR"

  if [ $# -eq 0 ]; then
    run_flink_sql "flink/flink.sql"
    run_starrocks_sql "starrocks/starrocks.sql"
  else
    for f in "$@"; do
      case "$f" in
        flink|flink.sql|flink/flink.sql)
          run_flink_sql "flink/flink.sql"
          ;;
        starrocks|starrocks.sql|starrocks/starrocks.sql)
          run_starrocks_sql "starrocks/starrocks.sql"
          ;;
        *)
          if [ -f "$f" ]; then
            if [[ "$f" == *flink* ]]; then
              run_flink_sql "$f"
            else
              run_starrocks_sql "$f"
            fi
          else
            echo "错误: 文件 $f 不存在"
          fi
          ;;
      esac
    done
  fi
}

# -----------------------------------------------------------------------------
# 主入口
# -----------------------------------------------------------------------------

usage() {
  echo "用法: $0 <子命令> [参数...]"
  echo ""
  echo "子命令:"
  echo "  install                安装所有依赖"
  echo "  fix                    修复 Flink 入湖"
  echo "  verify                 验证埋点数据链路（发送测试埋点后检查）"
  echo "  replay                 重放 test_data 中的神策日志"
  echo "  sync-metadata          转换并导入埋点元数据（MySQL -> PG）"
  echo "  reset                  清除数据并重建（含 flink.sql、starrocks.sql）"
  echo "  run-sql [flink|starrocks]  执行 SQL（无参数时执行 flink.sql + starrocks.sql）"
  echo "  download-starrocks-jars  仅下载 StarRocks 外部目录依赖"
  echo ""
  echo "示例:"
  echo "  $0 install"
  echo "  $0 fix"
  echo "  $0 run-sql"
}

CMD="${1:-}"
shift || true

case "$CMD" in
  install)
    cmd_install
    ;;
  fix)
    cmd_fix
    ;;
  verify)
    cmd_verify
    ;;
  replay)
    echo ">>> 开始重放测试日志..."
    python3 "$SCRIPT_DIR/replay_logs.py"
    ;;
  sync-metadata)
    cmd_sync_metadata
    ;;
  reset)
    cmd_reset
    ;;
  run-sql)
    cmd_run_sql "$@"
    ;;
  download-starrocks-jars)
    cmd_download_starrocks_jars
    ;;
  -h|--help|"")
    usage
    exit 0
    ;;
  *)
    echo "未知子命令: $CMD"
    usage
    exit 1
    ;;
esac
