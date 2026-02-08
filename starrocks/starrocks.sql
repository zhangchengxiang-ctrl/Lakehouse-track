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
