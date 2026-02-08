-- 合并执行脚本：01-04
-- 建议使用：./scripts/lakehouse.sh run-sql（或 sql-client stdin 模式）
-- 说明：部分版本在 -f 模式下会触发 only single statement supported
--
-- 架构：埋点 staging -> Paimon；CDC 数据库 -> Paimon；StarRocks 通过 External Catalog 读 Paimon
--
-- 并行度限制：单 TaskManager 2 slots，总并行度需 <= 2（最小资源模式）
SET 'parallelism.default' = '1';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '1min';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl' = '24 h'; -- 开启状态清理，防止 CDC 状态无限增长

-- ===================== 01-catalog.sql =====================
-- 1. 创建 Paimon Catalog（Hive Metastore）
DROP CATALOG IF EXISTS paimon_hms;

CREATE CATALOG paimon_hms WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://hive-metastore:9083',
    'warehouse' = 's3a://paimon-lake/paimon_data',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key' = 'minioadmin',
    's3.secret-key' = 'minioadmin',
    's3.path.style.access' = 'true',
    'lock.enabled' = 'false'
);

-- 2. 切换到该 Catalog 并创建显式数据库
USE CATALOG paimon_hms;
CREATE DATABASE IF NOT EXISTS ods;
USE ods;

-- ===================== 02-paimon-tables.sql =====================
-- 需在 USE ods 后执行
-- 按 event_group 分表，支持分级生命周期
-- 初始化：强制重建表（确保 schema 与脚本一致）
DROP TABLE IF EXISTS ods_events_core;
CREATE TABLE IF NOT EXISTS ods_events_core (
    `time` TIMESTAMP(3),
    distinct_id STRING,
    `event` STRING,
    `type` STRING,
    `project` STRING,
    `properties` STRING,
    ua_browser STRING,
    ua_os STRING,
    ua_device STRING,
    geoip STRING, -- 存储为 JSON 字符串
    redis_meta STRING,
    remote_addr STRING,
    event_group STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'bucket' = '1',
    'bucket-key' = 'distinct_id',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'parquet.compression' = 'zstd'
);
DROP TABLE IF EXISTS ods_events_trace;

CREATE TABLE IF NOT EXISTS ods_events_trace (
    `time` TIMESTAMP(3),
    distinct_id STRING,
    `event` STRING,
    `type` STRING,
    `project` STRING,
    `properties` STRING,
    ua_browser STRING,
    ua_os STRING,
    ua_device STRING,
    geoip STRING,
    redis_meta STRING,
    remote_addr STRING,
    event_group STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'bucket' = '1',
    'bucket-key' = 'distinct_id',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'parquet.compression' = 'zstd',
    'partition.expiration-time' = '30 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd'
);
DROP TABLE IF EXISTS ods_events_debug;

CREATE TABLE IF NOT EXISTS ods_events_debug (
    `time` TIMESTAMP(3),
    distinct_id STRING,
    `event` STRING,
    `type` STRING,
    `project` STRING,
    `properties` STRING,
    ua_browser STRING,
    ua_os STRING,
    ua_device STRING,
    geoip STRING,
    redis_meta STRING,
    remote_addr STRING,
    event_group STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'bucket' = '1',
    'bucket-key' = 'distinct_id',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'parquet.compression' = 'zstd',
    'partition.expiration-time' = '3 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd'
);

-- ===================== 03-ingestion.sql =====================
-- 3. 启动埋点入湖作业
-- 针对 MinIO 优化：增加 s3.path.style 配置

-- 初始化：强制重建 source（确保 schema 一致）
DROP TABLE IF EXISTS default_catalog.default_database.nginx_source;

CREATE TABLE default_catalog.default_database.nginx_source (
    `time` TIMESTAMP(3),
    distinct_id STRING,
    `event` STRING,
    `type` STRING,
    `project` STRING,
    `properties` STRING,
    ua_browser STRING,
    ua_os STRING,
    ua_device STRING,
    geoip STRING,
    redis_meta STRING,
    remote_addr STRING,
    event_group STRING,
    dt STRING,
    `hour` STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 's3a://paimon-lake/staging/',
    'format' = 'json',
    'source.monitor-interval' = '10 s',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- ===================== 04-cdc-pg-to-paimon.sql =====================
-- 4. CDC 统一走 Database -> Paimon -> StarRocks（数据先入湖，StarRocks 通过 External Catalog 读）
-- 源表：postgres/init/02-create-cdc-test.sql 中的 cdc_test_orders
USE CATALOG paimon_hms;
USE ods;
-- 初始化：强制重建 CDC 目标表
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

-- 使用 STATEMENT SET 合并提交任务，减少资源消耗（Flink 1.20+ 推荐语法）
EXECUTE STATEMENT SET
BEGIN
INSERT INTO paimon_hms.ods.ods_events_trace (
    `time`,
    distinct_id,
    `event`,
    `type`,
    `project`,
    `properties`,
    ua_browser,
    ua_os,
    ua_device,
    geoip,
    redis_meta,
    remote_addr,
    event_group,
    dt
)
SELECT
    `time`,
    distinct_id,
    `event`,
    `type`,
    `project`,
    `properties`,
    ua_browser,
    ua_os,
    ua_device,
    geoip,
    redis_meta,
    remote_addr,
    event_group,
    dt
FROM default_catalog.default_database.nginx_source
WHERE UPPER(event_group) = 'TRACE';

INSERT INTO paimon_hms.ods.ods_events_debug (
    `time`,
    distinct_id,
    `event`,
    `type`,
    `project`,
    `properties`,
    ua_browser,
    ua_os,
    ua_device,
    geoip,
    redis_meta,
    remote_addr,
    event_group,
    dt
)
SELECT
    `time`,
    distinct_id,
    `event`,
    `type`,
    `project`,
    `properties`,
    ua_browser,
    ua_os,
    ua_device,
    geoip,
    redis_meta,
    remote_addr,
    event_group,
    dt
FROM default_catalog.default_database.nginx_source
WHERE UPPER(event_group) = 'DEBUG';

INSERT INTO paimon_hms.ods.ods_events_core (
    `time`,
    distinct_id,
    `event`,
    `type`,
    `project`,
    `properties`,
    ua_browser,
    ua_os,
    ua_device,
    geoip,
    redis_meta,
    remote_addr,
    event_group,
    dt
)
SELECT
    `time`,
    distinct_id,
    `event`,
    `type`,
    `project`,
    `properties`,
    ua_browser,
    ua_os,
    ua_device,
    geoip,
    redis_meta,
    remote_addr,
    event_group,
    dt
FROM default_catalog.default_database.nginx_source
WHERE COALESCE(UPPER(event_group), 'CORE') = 'CORE';

INSERT INTO paimon_hms.ods.ods_orders_cdc SELECT * FROM sync_pg_orders;
END;
