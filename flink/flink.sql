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
    login_id STRING,
    anonymous_id STRING,
    original_id STRING,
    `event` STRING,
    `type` STRING,
    `project` STRING,
    `properties` STRING,
    properties_keys ARRAY<STRING>,
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

CREATE TEMPORARY VIEW nginx_source_norm AS
SELECT
    s.`time`,
    s.distinct_id,
    s.login_id,
    s.anonymous_id,
    s.original_id,
    s.`event`,
    s.`type`,
    s.`project`,
    s.`properties`,
    s.properties_keys,
    s.ua_browser,
    s.ua_os,
    s.ua_device,
    s.geoip,
    s.redis_meta,
    s.remote_addr,
    CASE
        WHEN s.event_group IS NOT NULL THEN UPPER(s.event_group)
        WHEN s.`event` LIKE 'debug_%' THEN 'DEBUG'
        WHEN s.`event` LIKE '$%' THEN 'CORE'
        ELSE 'TRACE'
    END AS event_group,
    COALESCE(s.dt, DATE_FORMAT(s.`time`, 'yyyy-MM-dd')) AS dt,
    COALESCE(s.`hour`, DATE_FORMAT(s.`time`, 'HH')) AS `hour`
FROM default_catalog.default_database.nginx_source s;

-- ========== 元数据（PostgreSQL） ==========
CREATE TEMPORARY TABLE meta_project (
    id INT,
    name STRING,
    is_auto_create INT,
    status INT,
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.project',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE meta_project_sink (
    id INT,
    name STRING,
    cname STRING,
    super_token STRING,
    normal_token STRING,
    is_auto_create INT,
    status INT,
    delete_time TIMESTAMP(3),
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.project',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE meta_event_define (
    id INT,
    project_id INT,
    name STRING,
    accepted INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.event_define',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE meta_event_define_sink (
    id INT,
    project_id INT,
    name STRING,
    accepted INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.event_define',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE meta_property_define (
    id INT,
    project_id INT,
    name STRING,
    data_type INT,
    is_in_use INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.property_define',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE meta_property_define_sink (
    id INT,
    project_id INT,
    name STRING,
    data_type INT,
    is_in_use INT,
    is_load INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.property_define',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE meta_event_property (
    event_id INT,
    property_id INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.event_property',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE user_id_mapping_sink (
    project_name STRING,
    distinct_id STRING,
    map_id STRING,
    map_type STRING,
    login_id STRING,
    load_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.user_id_mapping',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE meta_user_id_mapping (
    project_name STRING,
    distinct_id STRING,
    map_id STRING,
    map_type STRING,
    login_id STRING,
    load_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.user_id_mapping',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY TABLE meta_event_property_sink (
    id INT,
    event_id INT,
    property_id INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'table-name' = 'user_track.event_property',
    'username' = 'paimon',
    'password' = 'paimon123',
    'driver' = 'org.postgresql.Driver'
);

CREATE TEMPORARY VIEW valid_events AS
SELECT
    p.name AS project_name,
    e.name AS event_name,
    e.accepted AS accepted,
    p.is_auto_create AS is_auto_create
FROM meta_event_define e
JOIN meta_project p ON e.project_id = p.id
WHERE p.status = 1;

CREATE TEMPORARY VIEW new_projects AS
SELECT
    CAST(NULL AS INT) AS id,
    s.`project` AS name,
    NULL AS cname,
    NULL AS super_token,
    NULL AS normal_token,
    1 AS is_auto_create,
    1 AS status,
    NULL AS delete_time,
    CURRENT_TIMESTAMP AS create_time
FROM nginx_source_norm s
LEFT JOIN meta_project p ON s.`project` = p.name
WHERE p.id IS NULL
GROUP BY s.`project`;

CREATE TEMPORARY VIEW new_events AS
SELECT
    CAST(NULL AS INT) AS id,
    pm.id AS project_id,
    s.`event` AS name,
    1 AS accepted,
    CURRENT_TIMESTAMP AS create_time,
    CURRENT_TIMESTAMP AS update_time
FROM nginx_source_norm s
JOIN meta_project pm ON s.`project` = pm.name AND pm.status = 1 AND pm.is_auto_create = 1
LEFT JOIN meta_event_define ed ON ed.project_id = pm.id AND ed.name = s.`event`
WHERE ed.id IS NULL
GROUP BY pm.id, s.`event`;

CREATE TEMPORARY VIEW new_properties AS
SELECT
    CAST(NULL AS INT) AS id,
    pm.id AS project_id,
    prop_key AS name,
    CASE
        WHEN REGEXP_LIKE(COALESCE(JSON_VALUE(s.properties, CONCAT('$.', prop_key)), ''), '^[+-]?\\d*(\\.\\d+)?([eE][+-]?\\d+)?$') THEN 1
        WHEN REGEXP_LIKE(COALESCE(JSON_QUERY(s.properties, CONCAT('$.', prop_key)), ''), '^\\[') THEN 3
        WHEN REGEXP_LIKE(COALESCE(JSON_QUERY(s.properties, CONCAT('$.', prop_key)), ''), '^\\{') THEN 4
        WHEN REGEXP_LIKE(COALESCE(JSON_VALUE(s.properties, CONCAT('$.', prop_key)), ''), '^\\d{4}-\\d{2}-\\d{2}') THEN 5
        WHEN REGEXP_LIKE(LOWER(COALESCE(JSON_VALUE(s.properties, CONCAT('$.', prop_key)), '')), '^(true|false|1|0|yes|no|on|off)$') THEN 6
        ELSE 2
    END AS data_type,
    1 AS is_in_use,
    1 AS is_load,
    CURRENT_TIMESTAMP AS create_time,
    CURRENT_TIMESTAMP AS update_time
FROM nginx_source_norm s
JOIN meta_project pm ON s.`project` = pm.name AND pm.status = 1 AND pm.is_auto_create = 1
CROSS JOIN UNNEST(s.properties_keys) AS t(prop_key)
LEFT JOIN meta_property_define pd ON pd.project_id = pm.id AND pd.name = prop_key
WHERE pd.id IS NULL
GROUP BY pm.id, prop_key;

CREATE TEMPORARY VIEW new_event_properties AS
SELECT
    CAST(NULL AS INT) AS id,
    ed.id AS event_id,
    pd.id AS property_id
FROM nginx_source_norm s
JOIN meta_project pm ON s.`project` = pm.name AND pm.status = 1
JOIN meta_event_define ed ON ed.project_id = pm.id AND ed.name = s.`event`
JOIN meta_property_define pd ON pd.project_id = pm.id
CROSS JOIN UNNEST(s.properties_keys) AS t(prop_key)
WHERE pd.name = prop_key
GROUP BY ed.id, pd.id;

CREATE TEMPORARY VIEW valid_event_properties AS
SELECT
    p.name AS project_name,
    e.name AS event_name,
    ARRAY_AGG(pd.name) AS property_names
FROM meta_event_property ep
JOIN meta_event_define e ON ep.event_id = e.id
JOIN meta_property_define pd ON ep.property_id = pd.id
JOIN meta_project p ON e.project_id = p.id
WHERE p.status = 1 AND pd.is_in_use = 1
GROUP BY p.name, e.name;

CREATE TEMPORARY VIEW user_id_mapping_updates AS
SELECT
    s.project_name,
    s.distinct_id,
    s.map_id,
    s.map_type,
    s.login_id,
    CURRENT_TIMESTAMP AS load_time
FROM (
    SELECT
        `project` AS project_name,
        CASE
            WHEN `type` = 'track_signup' AND original_id IS NOT NULL AND original_id <> ''
                THEN original_id
            ELSE distinct_id
        END AS distinct_id,
        CASE
            WHEN `type` = 'track_signup' AND original_id IS NOT NULL AND original_id <> ''
                THEN distinct_id
            WHEN login_id IS NOT NULL AND login_id <> '' AND login_id <> '-1'
                THEN login_id
            ELSE distinct_id
        END AS map_id,
        CASE
            WHEN `type` = 'track_signup' AND original_id IS NOT NULL AND original_id <> '' AND distinct_id = original_id
                THEN 'self_related_id'
            WHEN `type` = 'track_signup' AND original_id IS NOT NULL AND original_id <> '' AND login_id IS NOT NULL AND distinct_id = login_id
                THEN 'login_id'
            WHEN `type` = 'track_signup' AND original_id IS NOT NULL AND original_id <> '' AND (login_id IS NULL OR login_id = '' OR login_id = '-1')
                THEN 'anonymous_id'
            WHEN login_id IS NOT NULL AND login_id <> '' AND login_id <> '-1'
                THEN 'login_id'
            ELSE 'anonymous_id'
        END AS map_type,
        login_id
    FROM nginx_source_norm
    WHERE `type` IN ('track_signup', 'track_id_bind')
) s
LEFT JOIN meta_user_id_mapping m
  ON m.project_name = s.project_name AND m.distinct_id = s.distinct_id
WHERE m.distinct_id IS NULL;

CREATE TEMPORARY VIEW project_map AS
SELECT id, name
FROM meta_project
WHERE status = 1;

CREATE TEMPORARY VIEW invalid_event_properties AS
SELECT DISTINCT
    s.`project` AS project_name,
    s.`event` AS event_name,
    s.distinct_id,
    s.`time`
FROM nginx_source_norm s
CROSS JOIN UNNEST(s.properties_keys) AS t(prop_key)
JOIN project_map pm ON s.`project` = pm.name
JOIN meta_property_define pd
  ON pd.project_id = pm.id
 AND pd.name = prop_key
 AND pd.is_in_use = 1
LEFT JOIN meta_event_define ed
  ON ed.project_id = pm.id
 AND ed.name = s.`event`
WHERE
  (
    pd.data_type = 1 AND NOT REGEXP_LIKE(JSON_VALUE(s.properties, CONCAT('$.', prop_key)), '^[+-]?\\d*(\\.\\d+)?([eE][+-]?\\d+)?$')
  )
  OR (
    pd.data_type = 3 AND NOT REGEXP_LIKE(COALESCE(JSON_QUERY(s.properties, CONCAT('$.', prop_key)), ''), '^\\[')
  )
  OR (
    pd.data_type = 4 AND NOT REGEXP_LIKE(COALESCE(JSON_QUERY(s.properties, CONCAT('$.', prop_key)), ''), '^\\{')
  )
  OR (
    pd.data_type = 5 AND NOT REGEXP_LIKE(COALESCE(JSON_VALUE(s.properties, CONCAT('$.', prop_key)), ''), '^\\d{4}-\\d{2}-\\d{2}')
  )
  OR (
    pd.data_type = 6 AND NOT REGEXP_LIKE(LOWER(COALESCE(JSON_VALUE(s.properties, CONCAT('$.', prop_key)), '')), '^(true|false|1|0|yes|no|on|off)$')
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
INSERT INTO meta_project_sink SELECT * FROM new_projects;

INSERT INTO meta_event_define_sink SELECT * FROM new_events;

INSERT INTO meta_property_define_sink SELECT * FROM new_properties;

INSERT INTO meta_event_property_sink SELECT * FROM new_event_properties;

INSERT INTO user_id_mapping_sink SELECT * FROM user_id_mapping_updates;

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
    s.`time`,
    s.distinct_id,
    s.`event`,
    s.`type`,
    s.`project`,
    s.`properties`,
    s.ua_browser,
    s.ua_os,
    s.ua_device,
    s.geoip,
    s.redis_meta,
    s.remote_addr,
    s.event_group,
    s.dt
FROM nginx_source_norm s
LEFT JOIN valid_events v
  ON s.`event` = v.event_name AND s.`project` = v.project_name
LEFT JOIN valid_event_properties vp
  ON s.`event` = vp.event_name AND s.`project` = vp.project_name
WHERE UPPER(s.event_group) = 'TRACE'
  AND (v.accepted = 1 OR v.is_auto_create = 1)
  AND (vp.property_names IS NULL OR s.properties_keys IS NULL OR CARDINALITY(ARRAY_EXCEPT(s.properties_keys, vp.property_names)) = 0)
  AND NOT EXISTS (
      SELECT 1
      FROM invalid_event_properties iep
      WHERE iep.project_name = s.`project`
        AND iep.event_name = s.`event`
        AND iep.distinct_id = s.distinct_id
        AND iep.`time` = s.`time`
  );

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
    s.`time`,
    s.distinct_id,
    s.`event`,
    s.`type`,
    s.`project`,
    s.`properties`,
    s.ua_browser,
    s.ua_os,
    s.ua_device,
    s.geoip,
    s.redis_meta,
    s.remote_addr,
    s.event_group,
    s.dt
FROM nginx_source_norm s
LEFT JOIN valid_events v
  ON s.`event` = v.event_name AND s.`project` = v.project_name
LEFT JOIN valid_event_properties vp
  ON s.`event` = vp.event_name AND s.`project` = vp.project_name
WHERE UPPER(s.event_group) = 'DEBUG'
  AND (v.accepted = 1 OR v.is_auto_create = 1)
  AND (vp.property_names IS NULL OR s.properties_keys IS NULL OR CARDINALITY(ARRAY_EXCEPT(s.properties_keys, vp.property_names)) = 0)
  AND NOT EXISTS (
      SELECT 1
      FROM invalid_event_properties iep
      WHERE iep.project_name = s.`project`
        AND iep.event_name = s.`event`
        AND iep.distinct_id = s.distinct_id
        AND iep.`time` = s.`time`
  );

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
    s.`time`,
    s.distinct_id,
    s.`event`,
    s.`type`,
    s.`project`,
    s.`properties`,
    s.ua_browser,
    s.ua_os,
    s.ua_device,
    s.geoip,
    s.redis_meta,
    s.remote_addr,
    s.event_group,
    s.dt
FROM nginx_source_norm s
LEFT JOIN valid_events v
  ON s.`event` = v.event_name AND s.`project` = v.project_name
LEFT JOIN valid_event_properties vp
  ON s.`event` = vp.event_name AND s.`project` = vp.project_name
WHERE COALESCE(UPPER(s.event_group), 'CORE') = 'CORE'
  AND (v.accepted = 1 OR v.is_auto_create = 1)
  AND (vp.property_names IS NULL OR s.properties_keys IS NULL OR CARDINALITY(ARRAY_EXCEPT(s.properties_keys, vp.property_names)) = 0)
  AND NOT EXISTS (
      SELECT 1
      FROM invalid_event_properties iep
      WHERE iep.project_name = s.`project`
        AND iep.event_name = s.`event`
        AND iep.distinct_id = s.distinct_id
        AND iep.`time` = s.`time`
  );

INSERT INTO paimon_hms.ods.ods_orders_cdc SELECT * FROM sync_pg_orders;
END;
