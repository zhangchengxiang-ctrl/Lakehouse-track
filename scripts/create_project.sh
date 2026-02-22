#!/usr/bin/env bash
# ============================================================
# 创建埋点项目（公共事件/属性自动共享种子表，无需额外初始化）
#
# 用法：
#   ./scripts/create_project.sh <项目名> [中文名]
#   make create-project ARGS="my_app 我的应用"
#
# 示例：
#   ./scripts/create_project.sh production 正式项目
#   ./scripts/create_project.sh staging
# ============================================================
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "用法: $0 <项目名> [中文名]"
  echo "示例: $0 production 正式项目"
  exit 1
fi

PROJECT_NAME="$1"
PROJECT_CNAME="${2:-$1}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

if [ -f "$ROOT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$ROOT_DIR/.env"
  set +a
fi

PG_HOST="${PG_HOST:-postgres}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_DB="${PG_DB:-postgres}"
PG_SCHEMA="${PG_SCHEMA:-StellarTrace}"

export PGPASSWORD="${PG_PASSWORD:-postgres}"

echo "正在创建项目: $PROJECT_NAME ($PROJECT_CNAME) ..."

RESULT=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
  -v ON_ERROR_STOP=1 -t -A -c \
  "INSERT INTO ${PG_SCHEMA}.track_project (name, cname, is_auto_create, status, create_time)
   VALUES ('${PROJECT_NAME}', '${PROJECT_CNAME}', 1, 1, NOW())
   RETURNING id;")

echo "项目创建成功！project_id = $RESULT"
