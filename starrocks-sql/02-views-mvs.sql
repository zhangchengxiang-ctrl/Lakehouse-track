-- 需先创建 dim_orders、dim_user_mapping（由 CDC 或批任务写入）
-- 若使用 Flink CDC，dim_orders 已存在；dim_user_mapping 需单独建表并初始化

-- 性能优化：开启 Data Cache（需 CN 节点挂载 NVMe）
SET GLOBAL enable_scan_datacache = true;

CREATE DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.dim_orders (
    order_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    amount DECIMAL(10,2),
    created_at DATETIME
) ENGINE=OLAP PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 16;

CREATE TABLE IF NOT EXISTS ods.dim_user_mapping (
    user_id VARCHAR(64) NOT NULL,
    id_int BIGINT,
    region VARCHAR(32),
    db_update_time DATETIME
) ENGINE=OLAP PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32;

-- 统一视图（合并三张 Paimon 表）
CREATE VIEW ods.ods_events AS
SELECT * FROM paimon_catalog.default.ods_events_core
UNION ALL
SELECT * FROM paimon_catalog.default.ods_events_trace
UNION ALL
SELECT * FROM paimon_catalog.default.ods_events_debug;

-- 资源组
CREATE RESOURCE GROUP IF NOT EXISTS mv_refresh_group
PROPERTIES ("cpu_core_limit" = "4", "mem_limit" = "20%", "concurrency_limit" = "2");

-- DWD 物化视图 (适配神策 distinct_id)
CREATE MATERIALIZED VIEW ods.dwd_user_full_track
REFRESH ASYNCHRONOUS EVERY (INTERVAL 10 MINUTE)
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

-- DWS 物化视图（需 id_int 非空）
CREATE MATERIALIZED VIEW ods.dws_daily_active_users
REFRESH ASYNCHRONOUS EVERY (INTERVAL 10 MINUTE)
AS
SELECT 
    to_date(event_time) AS dt,
    event_name,
    bitmap_union(to_bitmap(id_int)) AS uv_bitmap
FROM ods.dwd_user_full_track
WHERE id_int IS NOT NULL
GROUP BY dt, event_name;
