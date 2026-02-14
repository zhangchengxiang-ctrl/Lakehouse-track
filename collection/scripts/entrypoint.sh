#!/bin/bash
set -e

echo "[entrypoint] Collection node initializing..."
mkdir -p /var/log/nginx /var/lib/vector /data/meta

# 创建空 enrichment CSV（带表头），避免 Vector 启动失败
for f in projects.csv valid_events.csv valid_properties.csv; do
    if [ ! -f "/data/meta/$f" ]; then
        case "$f" in
            projects.csv)         echo "project_name,is_auto_create" > "/data/meta/$f" ;;
            valid_events.csv)     echo "project_name,event_name,accepted" > "/data/meta/$f" ;;
            valid_properties.csv) echo "project_name,property_name,data_type" > "/data/meta/$f" ;;
        esac
    fi
done

# 等待 PostgreSQL
PG_HOST="${PG_HOST:-postgres}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-paimon}"
PG_DB="${PG_DB:-paimon_db}"
export PGPASSWORD="${PG_PASSWORD:-paimon123}"

echo "[entrypoint] Waiting for PostgreSQL..."
for i in $(seq 1 60); do
    if pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" > /dev/null 2>&1; then
        echo "[entrypoint] PostgreSQL ready"
        break
    fi
    sleep 2
done

# 首次同步元数据
/opt/scripts/meta_sync.sh --once || echo "[entrypoint] Initial sync failed, will retry"

echo "[entrypoint] Starting supervisord..."
exec /usr/bin/supervisord -c /etc/supervisord.conf
