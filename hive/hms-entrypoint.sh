#!/bin/sh
set -e

echo "Waiting for Postgres..."
if command -v nc >/dev/null 2>&1; then
  for i in $(seq 1 60); do
    nc -z postgres 5432 && echo "Postgres is reachable." && break
    sleep 2
  done
else
  # Fallback if nc is unavailable
  echo "nc not found, sleeping 20s..."
  sleep 20
fi

export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_CLIENT_OPTS=" -Xmx1G \
  -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore \
  -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
  -Djavax.jdo.option.ConnectionUserName=paimon \
  -Djavax.jdo.option.ConnectionPassword=paimon123"

echo "Initializing HMS schema (idempotent)..."
if /opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
  echo "HMS schema already exists."
else
  /opt/hive/bin/schematool -dbType postgres -initSchema > /tmp/schematool.log 2>&1 || true
  if grep -q "already exists" /tmp/schematool.log; then
    echo "HMS schema already exists."
  elif /opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
    echo "HMS schema initialized."
  else
    cat /tmp/schematool.log
    echo "Schema initialization failed!"
    exit 1
  fi
fi

echo "Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore
