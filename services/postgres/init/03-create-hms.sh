#!/bin/bash
# Hive Metastore 专用库（业务表在默认库的 schema stellar_trace，见 02）
# 库名取自 .env 中的 HMS_DB，默认 metastore
set -euo pipefail

DB_NAME="${HMS_DB:-metastore}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  SELECT 'CREATE DATABASE "$DB_NAME"'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$DB_NAME')\gexec

  GRANT ALL PRIVILEGES ON DATABASE "$DB_NAME" TO "$POSTGRES_USER";
EOSQL

echo "✔ Database '$DB_NAME' ready (owner: $POSTGRES_USER)"
