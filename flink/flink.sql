-- ============================================================
-- Flink CDC 入湖：PostgreSQL → Paimon → StarRocks Paimon Catalog
--
-- 执行方式：
--   docker compose exec -T flink-jobmanager bash -c \
--     "cat /opt/flink/flink.sql | ./bin/sql-client.sh"
--   或通过 ./scripts/lakehouse.sh run-sql flink
--
-- 说明：
--   本文件仅负责 CDC 链路。埋点采集链路由 Vector → S3 TSV → StarRocks Pipe 处理，
--   不经过 Flink，详见 starrocks/starrocks.sql。
-- ============================================================

SET 'parallelism.default' = '1';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '1min';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl' = '24 h';

-- ========== 1. Paimon Catalog（Hive Metastore） ==========
DROP CATALOG IF EXISTS paimon_hms;

CREATE CATALOG paimon_hms WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://hive-metastore:9083',
    'warehouse' = 's3a://lakehouse/paimon_data',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key' = 'minioadmin',
    's3.secret-key' = 'minioadmin',
    's3.path.style.access' = 'true',
    'lock.enabled' = 'false'
);

USE CATALOG paimon_hms;
CREATE DATABASE IF NOT EXISTS ods;
USE ods;

-- ========== 2. CDC 目标表（Paimon） ==========
DROP TABLE IF EXISTS ods_orders_cdc;

CREATE TABLE IF NOT EXISTS ods_orders_cdc (
    order_id STRING,
    user_id STRING,
    amount DECIMAL(10, 2),
    created_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '1',
    'bucket-key' = 'order_id',
    'sink.parallelism' = '1'
);

-- ========== 3. CDC 源表（PostgreSQL） ==========
CREATE TEMPORARY TABLE IF NOT EXISTS sync_pg_orders (
    order_id STRING,
    user_id STRING,
    amount DECIMAL(10, 2),
    created_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'paimon',
    'password' = 'paimon123',
    'database-name' = 'paimon_db',
    'schema-name' = 'public',
    'table-name' = 'cdc_test_orders',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'paimon_orders_cdc_slot'
);

-- ========== 4. 启动 CDC 同步作业 ==========
INSERT INTO paimon_hms.ods.ods_orders_cdc SELECT * FROM sync_pg_orders;
