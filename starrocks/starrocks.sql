-- StarRocks 初始化：外部目录 + 视图 + 物化视图
-- 执行：./scripts/lakehouse.sh run-sql 或 mysql -h 127.0.0.1 -P 9030 -u root < starrocks/starrocks.sql
-- 前置：先执行 flink.sql 创建 ods_orders_cdc

-- ========== 1. 挂载 Paimon ==========
SET new_planner_optimize_timeout = 30000;
-- 外部 Catalog 由 run-sql 脚本创建并等待元数据就绪

-- ========== 2. 视图与物化视图 ==========
SET GLOBAL enable_scan_datacache = true;

CREATE DATABASE IF NOT EXISTS ods;

CREATE VIEW ods.dim_orders (order_id, user_id, amount, created_at) AS
SELECT order_id, user_id, amount, created_at
FROM paimon_catalog.ods.ods_orders_cdc;

CREATE TABLE ods.dim_user_mapping (
    user_id VARCHAR(64) NOT NULL,
    id_int BIGINT NOT NULL AUTO_INCREMENT COMMENT 'bitmap 用',
    region VARCHAR(32),
    db_update_time DATETIME
) ENGINE=OLAP PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 1;

CREATE VIEW ods.ods_events (
    `time`, distinct_id, `event`, `type`, project, properties,
    ua_browser, ua_os, ua_device, geoip, redis_meta, remote_addr, event_group, dt
) AS
SELECT
    `time`, distinct_id, `event`, `type`, project, properties,
    ua_browser, ua_os, ua_device, geoip, redis_meta, remote_addr, event_group, dt
FROM paimon_catalog.ods.ods_events_core
UNION ALL
SELECT
    `time`, distinct_id, `event`, `type`, project, properties,
    ua_browser, ua_os, ua_device, geoip, redis_meta, remote_addr, event_group, dt
FROM paimon_catalog.ods.ods_events_trace
UNION ALL
SELECT
    `time`, distinct_id, `event`, `type`, project, properties,
    ua_browser, ua_os, ua_device, geoip, redis_meta, remote_addr, event_group, dt
FROM paimon_catalog.ods.ods_events_debug;

CREATE RESOURCE GROUP mv_refresh_group
TO (db='ods')
WITH ('cpu_core_limit' = '1', 'mem_limit' = '10%', 'concurrency_limit' = '1');

CREATE MATERIALIZED VIEW ods.dwd_user_full_track
REFRESH ASYNC EVERY (INTERVAL 10 MINUTE)
AS
SELECT 
    e.`time` AS event_time,
    e.`event`,
    e.event_group,
    m.id_int,
    o.order_id,
    o.amount AS order_price
FROM ods.ods_events e
LEFT JOIN ods.dim_user_mapping m ON e.distinct_id = m.user_id
LEFT JOIN ods.dim_orders o ON e.distinct_id = o.user_id 
     AND ABS(TIMESTAMPDIFF(SECOND, e.`time`, o.created_at)) < 3600;

ALTER MATERIALIZED VIEW ods.dwd_user_full_track SET ("resource_group" = "mv_refresh_group");

CREATE MATERIALIZED VIEW ods.dws_daily_active_users
REFRESH ASYNC EVERY (INTERVAL 10 MINUTE)
AS
SELECT 
    to_date(event_time) AS dt,
    `event` AS event_name,
    bitmap_union(to_bitmap(id_int)) AS uv_bitmap
FROM ods.dwd_user_full_track
WHERE id_int IS NOT NULL
GROUP BY dt, `event`;

-- ========== 3. StellarTrace 初始化 ==========
-- 针对埋点读写多场景的设计（StarRocks OLAP 特性）
CREATE DATABASE IF NOT EXISTS stellar_trace;
USE stellar_trace;

CREATE TABLE stellar_trace.events (
    project_name VARCHAR(100) NOT NULL comment '项目名称',
    record_type VARCHAR(64) NOT NULL comment '记录类型',
    event_time DATETIME NOT NULL comment '事件时间',
    event_date DATE NOT NULL comment '事件日期(用于分区)',
    event_name VARCHAR(256) NOT NULL comment '事件名称',
    log_id VARCHAR(256) NOT NULL comment '日志ID',
    distinct_id VARCHAR(256) NOT NULL comment '用户唯一标识',
    user_id VARCHAR(256) comment '登录ID/账号ID',
    session_id VARCHAR(256) comment '会话ID',
    device_id VARCHAR(256) comment '设备ID',
    success BOOLEAN comment '解析是否成功',
    err_msg VARCHAR(5000) comment '错误信息',
    log_time DATETIME comment '日志时间',
    parser_time DATETIME comment '解析时间',
    meta_validate_time DATETIME comment '元数据验证时间',
    id_validate_time DATETIME comment 'ID验证时间',
    load_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP comment '数据加载时间',
    properties JSON NOT NULL comment '事件属性',
    -- 低基数字段使用位图索引
    INDEX idx_event_name (event_name) USING BITMAP comment '事件名称位图索引',
    INDEX idx_record_type (record_type) USING BITMAP comment '记录类型位图索引',
    INDEX idx_project_name (project_name) USING BITMAP comment '项目名称位图索引'
)
DUPLICATE KEY (project_name, event_date, event_time, event_name, log_id, distinct_id)
PARTITION BY RANGE (event_date) ()
DISTRIBUTED BY HASH(distinct_id) BUCKETS 16
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "7",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1",
    "compression" = "LZ4",
    "bloom_filter_columns" = "distinct_id,log_id,user_id,session_id,device_id"
);

-- 可选：属性键值表，便于按属性名过滤与回溯
CREATE TABLE stellar_trace.event_properties_kv (
    project_name VARCHAR(100) NOT NULL comment '项目名称',
    event_date DATE NOT NULL comment '事件日期(用于分区)',
    event_time DATETIME NOT NULL comment '事件时间',
    event_name VARCHAR(256) NOT NULL comment '事件名称',
    log_id VARCHAR(256) NOT NULL comment '日志ID',
    distinct_id VARCHAR(256) NOT NULL comment '用户唯一标识',
    prop_key VARCHAR(128) NOT NULL comment '属性名',
    prop_value_string VARCHAR(1000) comment '字符串属性值',
    prop_value_number DOUBLE comment '数值属性值',
    prop_value_bool BOOLEAN comment '布尔属性值',
    prop_value_json JSON comment '复杂属性值',
    load_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP comment '数据加载时间',
    INDEX idx_prop_key (prop_key) USING BITMAP comment '属性名位图索引',
    INDEX idx_event_name (event_name) USING BITMAP comment '事件名称位图索引',
    INDEX idx_project_name (project_name) USING BITMAP comment '项目名称位图索引'
)
DUPLICATE KEY (project_name, event_date, event_time, event_name, log_id, distinct_id, prop_key)
PARTITION BY RANGE (event_date) ()
DISTRIBUTED BY HASH(distinct_id) BUCKETS 16
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "7",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1",
    "compression" = "LZ4",
    "bloom_filter_columns" = "distinct_id,log_id,prop_key"
);

-- 统一身份映射，支持多类型 ID 关联
CREATE TABLE stellar_trace.user_id_mapping (
    project_name VARCHAR(100) NOT NULL comment '项目名称',
    distinct_id VARCHAR(256) NOT NULL comment '匿名ID',
    map_type VARCHAR(64) NOT NULL comment '映射类型(如login_id/device_id)',
    map_id VARCHAR(256) NOT NULL comment '映射ID',
    is_login BOOLEAN DEFAULT FALSE comment '是否登录态',
    first_seen_time DATETIME comment '首次出现时间',
    last_seen_time DATETIME comment '最近出现时间',
    load_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP comment '数据加载时间',
    INDEX idx_map_id (map_id) USING BITMAP comment '映射ID位图索引',
    INDEX idx_map_type (map_type) USING BITMAP comment '映射类型位图索引',
    INDEX idx_project_name (project_name) USING BITMAP comment '项目名称位图索引'
)
PRIMARY KEY (project_name, distinct_id, map_type, map_id)
DISTRIBUTED BY HASH(distinct_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "compression" = "LZ4"
);

-- 用户画像与统计，按主键更新
CREATE TABLE stellar_trace.user_profile (
    project_name VARCHAR(100) NOT NULL comment '项目名称',
    user_id VARCHAR(256) NOT NULL comment '登录ID/账号ID',
    distinct_id VARCHAR(256) comment '当前匿名ID',
    first_event_time DATETIME comment '首次事件时间',
    last_event_time DATETIME comment '最近事件时间',
    total_event_count BIGINT NOT NULL DEFAULT '0' comment '事件总数',
    session_count BIGINT NOT NULL DEFAULT '0' comment '会话数',
    properties JSON comment '用户属性',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP comment '更新时间',
    INDEX idx_distinct_id (distinct_id) USING BITMAP comment '匿名ID位图索引'
)
PRIMARY KEY (project_name, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "compression" = "LZ4"
);
