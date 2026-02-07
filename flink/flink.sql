-- 合并执行脚本：01-04
-- 建议使用: docker compose exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/flink.sql
--
-- 架构：埋点 staging -> Paimon；CDC 数据库 -> Paimon；StarRocks 通过 External Catalog 读 Paimon
--
-- 并行度限制：单 TaskManager 2 slots，总并行度需 <= 2（最小资源模式）
SET 'parallelism.default' = '1';

-- ===================== 01-catalog.sql =====================
-- 1. 创建 Paimon Catalog（filesystem metastore，与 StarRocks 读法一致）
DROP CATALOG IF EXISTS paimon_jdbc;
CREATE CATALOG paimon_jdbc WITH (
    'type' = 'paimon',
    'warehouse' = 's3://paimon-lake/data/',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key' = 'minioadmin',
    's3.secret-key' = 'minioadmin',
    's3.path.style.access' = 'true',
    'lock.enabled' = 'false'
);

-- 2. 切换到该 Catalog
USE CATALOG paimon_jdbc;

-- ===================== 02-paimon-tables.sql =====================
-- 需在 USE CATALOG paimon_jdbc 后执行
-- 按 event_group 分表，支持分级生命周期
CREATE TABLE IF NOT EXISTS ods_events_core (
    `time` TIMESTAMP(3),
    distinct_id STRING,
    `event` STRING,
    `type` STRING,
    `project` STRING,
    `properties` STRING,
    ua_browser STRING,
    ua_os STRING,
    geoip STRING, -- 存储为 JSON 字符串
    redis_meta STRING,
    remote_addr STRING,
    event_group STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'bucket' = '1',
    'bucket-key' = 'distinct_id',
    'sink.parallelism' = '1',
    'compaction.parallelism' = '1'
);

CREATE TABLE IF NOT EXISTS ods_events_trace (
    `time` TIMESTAMP(3),
    distinct_id STRING,
    `event` STRING,
    `type` STRING,
    `project` STRING,
    `properties` STRING,
    ua_browser STRING,
    ua_os STRING,
    geoip STRING,
    redis_meta STRING,
    remote_addr STRING,
    event_group STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'bucket' = '1',
    'bucket-key' = 'distinct_id',
    'partition.expiration-time' = '30 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd'
);

CREATE TABLE IF NOT EXISTS ods_events_debug (
    `time` TIMESTAMP(3),
    distinct_id STRING,
    `event` STRING,
    `type` STRING,
    `project` STRING,
    `properties` STRING,
    ua_browser STRING,
    ua_os STRING,
    geoip STRING,
    redis_meta STRING,
    remote_addr STRING,
    event_group STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'bucket' = '1',
    'bucket-key' = 'distinct_id',
    'partition.expiration-time' = '3 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd'
);

-- ===================== 03-ingestion.sql =====================
-- 3. 启动埋点入湖作业
-- 针对 MinIO 优化：增加 s3.path.style 配置
SET 'execution.checkpointing.interval' = '1min';

-- 若需应用 source.path.regex-pattern 等新配置，需先 DROP 再 CREATE
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
    geoip STRING,
    redis_meta STRING,
    remote_addr STRING,
    event_group STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 's3a://paimon-lake/staging/',
    'format' = 'json',
    'source.monitor-interval' = '10 s',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- ===================== 04-cdc-pg-to-paimon.sql =====================
-- 4. CDC 统一走 Database -> Paimon -> StarRocks（数据先入湖，StarRocks 通过 External Catalog 读）
-- 源表：postgres/init/02-create-cdc-test.sql 中的 cdc_test_orders
USE CATALOG paimon_jdbc;

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

-- 使用 STATEMENT SET 合并提交任务，减少资源消耗
BEGIN STATEMENT SET;
INSERT INTO ods_events_core (
    `time`,
    distinct_id,
    `event`,
    `type`,
    `project`,
    `properties`,
    ua_browser,
    ua_os,
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
    geoip,
    redis_meta,
    remote_addr,
    event_group,
    DATE_FORMAT(`time`, 'yyyy-MM-dd') as dt
FROM default_catalog.default_database.nginx_source;

INSERT INTO ods_orders_cdc SELECT * FROM sync_pg_orders;
END;
