#!/bin/bash
# ============================================================
# 元数据同步：每 30 秒从 PostgreSQL 导出 CSV → Vector enrichment_tables
# 文件更新后 Vector 自动热重载（inotify）
# ============================================================

set -e

META_DIR="/data/meta"
INTERVAL="${META_SYNC_INTERVAL:-30}"

PG_HOST="${PG_HOST:-postgres}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-paimon}"
PG_DB="${PG_DB:-paimon_db}"
export PGPASSWORD="${PG_PASSWORD:-paimon123}"

PG_CONN="-h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB"

mkdir -p "$META_DIR"

sync_once() {
    # 1) 项目列表
    {
        echo "project_name,is_auto_create"
        psql $PG_CONN -t -A -F',' -c \
            "SELECT name, is_auto_create FROM user_track.project WHERE status = 1" 2>/dev/null || true
    } > "${META_DIR}/projects.csv.tmp"
    mv -f "${META_DIR}/projects.csv.tmp" "${META_DIR}/projects.csv"

    # 2) 有效事件（项目+事件+accepted）
    {
        echo "project_name,event_name,accepted"
        psql $PG_CONN -t -A -F',' -c \
            "SELECT p.name, e.name, e.accepted
             FROM user_track.event_define e
             JOIN user_track.project p ON e.project_id = p.id
             WHERE p.status = 1" 2>/dev/null || true
    } > "${META_DIR}/valid_events.csv.tmp"
    mv -f "${META_DIR}/valid_events.csv.tmp" "${META_DIR}/valid_events.csv"

    # 3) 有效属性（项目+属性+数据类型）
    {
        echo "project_name,property_name,data_type"
        psql $PG_CONN -t -A -F',' -c \
            "SELECT p.name, pd.name, pd.data_type
             FROM user_track.property_define pd
             JOIN user_track.project p ON pd.project_id = p.id
             WHERE p.status = 1 AND pd.is_in_use = 1" 2>/dev/null || true
    } > "${META_DIR}/valid_properties.csv.tmp"
    mv -f "${META_DIR}/valid_properties.csv.tmp" "${META_DIR}/valid_properties.csv"

    echo "[meta-sync] $(date '+%H:%M:%S') CSV 同步完成 (projects=$(wc -l < ${META_DIR}/projects.csv) events=$(wc -l < ${META_DIR}/valid_events.csv) props=$(wc -l < ${META_DIR}/valid_properties.csv))"
}

# 支持 --once 参数：仅执行一次（用于 entrypoint 初始化）
if [ "$1" = "--once" ]; then
    sync_once
    exit 0
fi

# 循环同步
while true; do
    sync_once
    sleep "$INTERVAL"
done
