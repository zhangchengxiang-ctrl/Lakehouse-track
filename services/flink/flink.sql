-- ============================================================
-- Flink CDC 入湖：PostgreSQL → Paimon → StarRocks Paimon Catalog
--
-- 执行方式：make run-sql ARGS=flink
--
-- 本文件仅负责 CDC 链路。埋点采集链路由 Vector → S3 TSV → StarRocks Pipe 处理，
-- 不经过 Flink，详见 services/starrocks/starrocks.sql。
--
-- 注意：Flink SQL 不支持环境变量。以下 S3/PG 连接参数使用 Docker 内部默认值，
-- HMS URI 由 lakehouse.sh 在运行时通过 sed 注入。如需修改请同步更新 .env。
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
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'postgres',
    'schema-name' = 'cdc_test',
    'table-name' = 'cdc_test_orders',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'paimon_orders_cdc_slot'
);

-- ========== 4. 启动 CDC 同步作业 ==========
INSERT INTO paimon_hms.ods.ods_orders_cdc SELECT * FROM sync_pg_orders;
