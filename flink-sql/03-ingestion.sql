-- 需先执行 01-catalog.sql、02-paimon-tables.sql
-- 每个 INSERT 单独提交为一个 Flink Job

SET execution.checkpointing.interval = 5min;
-- 开启仅写入模式，由独立任务负责 Compaction
SET 'paimon.write-only' = 'true';

CREATE TEMPORARY TABLE staging_logs (
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
    is_valid BOOLEAN
) WITH (
    'connector' = 'filesystem',
    'path' = 's3://paimon-lake/staging/',
    'format' = 'parquet',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key' = 'minioadmin',
    's3.secret-key' = 'minioadmin',
    's3.path.style.access' = 'true',
    'source.monitor-interval' = '60 s'
);

-- Job 1: CORE（单独提交）
INSERT INTO ods_events_core
SELECT `time`, distinct_id, `event`, `type`, `project`, `properties`, 
       ua_browser, ua_os, geoip, redis_meta, remote_addr, event_group,
       DATE_FORMAT(`time`, 'yyyy-MM-dd') as dt
FROM staging_logs WHERE (event_group = 'CORE' OR event_group IS NULL OR event_group = '') AND is_valid = true;

-- Job 2: TRACE（单独提交）
-- INSERT INTO ods_events_trace
-- SELECT `time`, distinct_id, `event`, `type`, `project`, `properties`, 
--        ua_browser, ua_os, geoip, redis_meta, remote_addr, event_group,
--        DATE_FORMAT(`time`, 'yyyy-MM-dd') as dt
-- FROM staging_logs WHERE event_group = 'TRACE' AND is_valid = true;

-- Job 3: DEBUG（单独提交）
-- INSERT INTO ods_events_debug
-- SELECT `time`, distinct_id, `event`, `type`, `project`, `properties`, 
--        ua_browser, ua_os, geoip, redis_meta, remote_addr, event_group,
--        DATE_FORMAT(`time`, 'yyyy-MM-dd') as dt
-- FROM staging_logs WHERE event_group = 'DEBUG' AND is_valid = true;
