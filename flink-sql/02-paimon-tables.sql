-- 需在 USE CATALOG paimon_jdbc 后执行
-- 按 event_group 分表，支持分级生命周期

CREATE TABLE ods_events_core (
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
    event_group STRING
) PARTITIONED BY (dt STRING)
WITH (
    'bucket' = '4',
    'sink.parallelism' = '4',
    'compaction.parallelism' = '2'
);

CREATE TABLE ods_events_trace (
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
) PARTITIONED BY (dt STRING)
WITH (
    'bucket' = '2',
    'partition.expiration-time' = '30 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd'
);

CREATE TABLE ods_events_debug (
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
) PARTITIONED BY (dt STRING)
WITH (
    'bucket' = '1',
    'partition.expiration-time' = '3 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd'
);
