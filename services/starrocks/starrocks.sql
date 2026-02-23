-- ============================================================
-- StarRocks 统一初始化：埋点采集 + CDC 双链路（存算分离模式）
--
-- 架构概览：
--   ods_events（生成列）→ dws_daily_event_stats（MV 聚合）
--                       → dwd_users（TASK 聚合）
--
--   生成列替代 MV：14 个高频维度写入时自动从 JSON 提取并物理存储
--   无中间表、零延迟、查询等同普通列
--
-- 存储分层：
--   热数据（≤30天）→ CN DataCache 本地缓存  ~ms
--   冷数据（>30天）→ S3 读取              ~100ms
--
-- 生命周期管理：
--   CORE  事件 → 365 天（分区级 TTL，dynamic_partition.start）
--   TRACE 事件 → 180 天（定时 DELETE 任务）
--   DEBUG 事件 → 30 天 （定时 DELETE 任务）
--
-- 数据链路：
--   埋点：Vector → S3 (TSV.gz) → Pipe + FILES() → ods_events
--   CDC ：Flink → Paimon → paimon_catalog / pg_catalog JDBC 直读
--
-- 执行：mysql -h 127.0.0.1 -P 9030 -u root < services/starrocks/starrocks.sql
-- 前置：docker compose up -d --build
-- ============================================================

-- 全局开启 DataCache 扫描（存算分离核心开关）
SET GLOBAL enable_scan_datacache = true;
-- 开启 DataCache 异步填充（查询完成后异步写入缓存，不阻塞查询返回）
SET GLOBAL enable_populate_datacache = true;
SET new_planner_optimize_timeout = 30000;

-- ---- 查询缓存（重复查询 <1ms 返回，Dashboard/报表场景提升显著）----
SET GLOBAL enable_query_cache = true;
-- 单个缓存条目上限 4MB，超出的查询结果不缓存
SET GLOBAL query_cache_entry_max_bytes = 4194304;
-- 单个缓存条目最大行数
SET GLOBAL query_cache_entry_max_rows = 100000;

-- ---- Tablet 内部并行（单 tablet 数据量大时自动拆分并行扫描）----
SET GLOBAL enable_tablet_internal_parallel = true;

-- ========== 1. ODS 数据库 ==========
CREATE DATABASE IF NOT EXISTS ods;
USE ods;

-- ========== 2. ODS 事件表（生成列 + 冷热分层 + 生命周期管理）==========
--
-- 设计理念：
--   · 原始数据列（11 列）：排序键 + 身份 + 时间 + properties JSON
--   · 生成列（14 列）：写入时自动从 properties JSON 提取高频维度
--     无中间 MV，零延迟，可加 BITMAP 索引，查询等同普通列
--   · 扩展维度：ALTER TABLE ADD COLUMN ... AS (expression)
--
-- 冷热分层（存算分离）：
--   · 热数据（≤30天）→ CN DataCache 本地缓存，~ms 级延迟
--   · 冷数据（>30天）→ S3 读取，~100ms 级，按需缓存
--
-- 生命周期管理（按 event_group 差异化保留）：
--   ┌───────────────┬──────────┬──────────────────────────────┐
--   │ event_group   │ 保留期限  │ 实现方式                      │
--   ├───────────────┼──────────┼──────────────────────────────┤
--   │ CORE          │ 365 天   │ dynamic_partition.start=-365  │
--   │ TRACE         │ 180 天   │ 定时 DELETE 清理任务           │
--   │ DEBUG         │ 30 天    │ 定时 DELETE 清理任务           │
--   └───────────────┴──────────┴──────────────────────────────┘
--   分区级 TTL = 365 天（最长保留），超期分区自动删除
--   短生命周期的 TRACE/DEBUG 由定时任务提前清理行数据
--
-- 排序键 (dt, event, event_group, project)：
--   · dt         — 分区键，分区裁剪
--   · event      — 中基数(~数百)，Zone Map 跳过无关 block
--   · event_group — 极低基数(CORE/TRACE/DEBUG)，过滤噪声
--   · project    — 低基数(~数十)，多项目筛选
--   distinct_id 基数极高，用 HASH 分桶 + Bloom Filter 加速点查
--
CREATE TABLE IF NOT EXISTS ods.ods_events (
    -- ---- 排序键（DUPLICATE KEY）----
    dt DATE NOT NULL COMMENT '日期分区键',
    `event` VARCHAR(256) NOT NULL COMMENT '事件名称',
    event_group VARCHAR(16) NOT NULL COMMENT 'CORE/TRACE/DEBUG',
    project VARCHAR(128) NOT NULL DEFAULT 'default' COMMENT '项目名称',
    -- ---- 身份 ----
    distinct_id VARCHAR(256) NOT NULL COMMENT '用户标识',
    login_id VARCHAR(256) COMMENT '登录ID',
    anonymous_id VARCHAR(256) COMMENT '匿名ID',
    original_id VARCHAR(256) COMMENT '原始ID（仅 signup/bind 事件）',
    `type` VARCHAR(64) COMMENT '事件类型: track/profile_set/track_signup',
    -- ---- 时间 ----
    event_time DATETIME NOT NULL COMMENT '事件时间',
    -- ---- 全量属性 JSON ----
    properties JSON COMMENT '全量事件属性（预置+GeoIP+UA+自定义）',
    -- ---- 生成列：写入时自动从 properties 提取，物理存储，查询零开销 ----
    -- SDK
    lib VARCHAR(32) AS CAST(json_query(properties, '$."$lib"') AS VARCHAR(32)) COMMENT 'SDK 类型',
    lib_version VARCHAR(32) AS CAST(json_query(properties, '$."$lib_version"') AS VARCHAR(32)) COMMENT 'SDK 版本',
    -- 身份状态
    is_login_id VARCHAR(8) AS CAST(json_query(properties, '$."$is_login_id"') AS VARCHAR(8)) COMMENT '是否登录用户',
    -- 设备
    device_type VARCHAR(32) AS CAST(json_query(properties, '$."$device_type"') AS VARCHAR(32)) COMMENT '设备类型',
    os VARCHAR(64) AS CAST(json_query(properties, '$."$os"') AS VARCHAR(64)) COMMENT '操作系统',
    os_version VARCHAR(32) AS CAST(json_query(properties, '$."$os_version"') AS VARCHAR(32)) COMMENT '系统版本',
    browser VARCHAR(64) AS CAST(json_query(properties, '$."$browser"') AS VARCHAR(64)) COMMENT '浏览器',
    browser_version VARCHAR(32) AS CAST(json_query(properties, '$."$browser_version"') AS VARCHAR(32)) COMMENT '浏览器版本',
    -- 地理
    ip VARCHAR(64) AS CAST(json_query(properties, '$."$ip"') AS VARCHAR(64)) COMMENT '客户端IP',
    country VARCHAR(64) AS CAST(json_query(properties, '$."$country"') AS VARCHAR(64)) COMMENT '国家',
    province VARCHAR(64) AS CAST(json_query(properties, '$."$province"') AS VARCHAR(64)) COMMENT '省份',
    city VARCHAR(64) AS CAST(json_query(properties, '$."$city"') AS VARCHAR(64)) COMMENT '城市',
    -- 流量来源
    url_host VARCHAR(256) AS CAST(json_query(properties, '$."$url_host"') AS VARCHAR(256)) COMMENT '页面域名',
    traffic_source VARCHAR(64) AS CAST(json_query(properties, '$."$latest_traffic_source_type"') AS VARCHAR(64)) COMMENT '流量来源类型',
    -- ---- 二级索引（含生成列索引）----
    INDEX idx_event (`event`) USING BITMAP,
    INDEX idx_event_group (event_group) USING BITMAP,
    INDEX idx_project (project) USING BITMAP,
    INDEX idx_lib (lib) USING BITMAP,
    INDEX idx_country (country) USING BITMAP,
    INDEX idx_device_type (device_type) USING BITMAP
)
DUPLICATE KEY (dt, `event`, event_group, project)
PARTITION BY RANGE (dt) ()
DISTRIBUTED BY HASH(distinct_id) BUCKETS 16
PROPERTIES (
    -- ---- 动态分区（建表时关闭，手动建完历史分区后开启）----
    "dynamic_partition.enable" = "false",
    "dynamic_partition.time_unit" = "DAY",
    -- 保留 365 天（CORE 最长生命周期），超期分区自动删除
    -- TRACE(180天)/DEBUG(30天) 由定时 DELETE 任务提前清理
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    -- ---- 存储与压缩 ----
    "replication_num" = "1",
    "compression" = "ZSTD",
    -- ---- 索引 ----
    "bloom_filter_columns" = "distinct_id,login_id",
    -- ---- 冷热分层 ----
    -- 热数据（≤30天）：CN DataCache 本地缓存，查询 ~ms 级
    -- 冷数据（>30天）：S3 读取 ~100ms 级，首次访问后按需缓存
    "datacache.enable" = "true",
    "datacache.partition_duration" = "30 day"
);

-- 动态分区在建表后开启：
--   自动创建未来 3 天分区，自动删除超过 365 天的分区
--   如需加载历史数据，先手动 ADD PARTITION 再灌入
ALTER TABLE ods.ods_events SET ("dynamic_partition.enable" = "true");

-- ========== 3. Pipe 定义见 starrocks-pipes.sql ==========
-- PIPE 依赖 S3 路径中存在 .tsv.gz 文件，首次启动时由 lakehouse.sh 延迟创建

-- ========== 4. 用户 ID Mapping 内表 ==========
-- Vector 写入路径：s3://lakehouse/track/id_mapping/dt={date}/hour={hour}/{uuid}.tsv.gz
-- TSV 列顺序：$1 project | $2 map_type | $3 anonymous_id | $4 login_id |
--             $5 distinct_id | $6 original_id | $7 event_time | $8 dt | $9 hour
-- 注：anonymous_id 最大 64 字节，超出 PRIMARY KEY 128 字节总长限制
-- 改用 DUPLICATE KEY 模型 + BITMAP 索引替代主键去重
CREATE TABLE IF NOT EXISTS ods.ods_id_mapping (
    project VARCHAR(128) NOT NULL COMMENT '项目',
    anonymous_id VARCHAR(256) NOT NULL COMMENT '匿名ID',
    login_id VARCHAR(256) NOT NULL COMMENT '登录ID',
    map_type VARCHAR(32) NOT NULL COMMENT 'signup/login/id_bind/id_unbind',
    distinct_id VARCHAR(256) COMMENT '最终身份ID',
    original_id VARCHAR(256) COMMENT '原始ID',
    first_seen DATETIME COMMENT '首次关联时间',
    last_seen DATETIME COMMENT '最近关联时间',
    INDEX idx_login (login_id) USING BITMAP,
    INDEX idx_project (project) USING BITMAP,
    INDEX idx_map_type (map_type) USING BITMAP
)
DUPLICATE KEY (project, anonymous_id, login_id)
DISTRIBUTED BY HASH(anonymous_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "compression" = "LZ4",
    "datacache.enable" = "true"
);

-- ========== 5. CDC 链路：JDBC 外部目录 ==========
-- 直读 PostgreSQL，用于轻量级维度查询
-- Paimon Catalog 由 scripts/lakehouse.sh run-sql 脚本创建，此处不重复定义
CREATE EXTERNAL CATALOG IF NOT EXISTS pg_catalog
PROPERTIES (
    "type" = "jdbc",
    "user" = "postgres",
    "password" = "postgres",
    "jdbc_uri" = "jdbc:postgresql://postgres:5432/postgres",
    "driver_url" = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar",
    "driver_class" = "org.postgresql.Driver"
);

-- ========== 6. DWS 每日事件统计（直接聚合 ods_events 生成列）==========
-- 生成列已在写入时物理存储，聚合无需 JSON 解析，等同普通列
-- 分区对齐增量刷新，partition_refresh_number=3 防止积压雪崩
CREATE MATERIALIZED VIEW IF NOT EXISTS ods.dws_daily_event_stats
PARTITION BY (dt)
REFRESH ASYNC EVERY (INTERVAL 10 MINUTE)
PROPERTIES (
    "partition_refresh_number" = "3"
)
AS
SELECT
    dt,
    `event`,
    event_group,
    project,
    lib,
    country,
    COUNT(*) AS event_count,
    COUNT(DISTINCT distinct_id) AS uv
FROM ods.ods_events
GROUP BY dt, `event`, event_group, project, lib, country;

-- ========== 8. DWD 用户宽表 ==========
-- 面向用户分群、画像查询、Cohort 分析
-- PRIMARY KEY 模型，由定时 TASK 从 ods_events 聚合生成
CREATE TABLE IF NOT EXISTS ods.dwd_users (
    project VARCHAR(128) NOT NULL COMMENT '项目',
    distinct_id VARCHAR(256) NOT NULL COMMENT '用户标识',
    -- 身份
    login_id VARCHAR(256) COMMENT '最近登录ID',
    anonymous_id VARCHAR(256) COMMENT '最近匿名ID',
    -- 生命周期
    first_event_time DATETIME COMMENT '首次事件时间',
    last_event_time DATETIME COMMENT '最近事件时间',
    signup_time DATETIME COMMENT '注册时间（track_signup）',
    active_days INT COMMENT '活跃天数',
    total_events BIGINT COMMENT '总事件数',
    -- 首次归因
    first_lib VARCHAR(32) COMMENT '首次 SDK 类型',
    first_country VARCHAR(64) COMMENT '首次国家',
    first_city VARCHAR(64) COMMENT '首次城市',
    -- 最近属性
    last_lib VARCHAR(32) COMMENT '最近 SDK 类型',
    last_os VARCHAR(64) COMMENT '最近操作系统',
    last_device_type VARCHAR(32) COMMENT '最近设备类型',
    last_country VARCHAR(64) COMMENT '最近国家',
    last_city VARCHAR(64) COMMENT '最近城市',
    last_ip VARCHAR(64) COMMENT '最近IP',
    -- 用户属性（来自 profile_set/profile_set_once 事件的 properties）
    user_properties JSON COMMENT '用户自定义属性',
    updated_at DATETIME COMMENT '最近更新时间'
) PRIMARY KEY (project, distinct_id)
DISTRIBUTED BY HASH(distinct_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "datacache.enable" = "true"
);

-- ========== 9. 定时任务：刷新 dwd_users ==========
-- 每 10 分钟从 ods_events 聚合用户画像，INSERT OVERWRITE 全量刷新
-- 直接使用生成列（lib/os/country 等），无需 json_query
SUBMIT TASK refresh_dwd_users
SCHEDULE EVERY(INTERVAL 10 MINUTE)
AS
INSERT OVERWRITE ods.dwd_users
SELECT
    e.project,
    e.distinct_id,
    ANY_VALUE(e.login_id) AS login_id,
    ANY_VALUE(e.anonymous_id) AS anonymous_id,
    MIN(e.event_time) AS first_event_time,
    MAX(e.event_time) AS last_event_time,
    MIN(CASE WHEN e.`type` = 'track_signup' THEN e.event_time END) AS signup_time,
    COUNT(DISTINCT e.dt) AS active_days,
    COUNT(*) AS total_events,
    MIN_BY(e.lib, e.event_time) AS first_lib,
    MIN_BY(e.country, e.event_time) AS first_country,
    MIN_BY(e.city, e.event_time) AS first_city,
    MAX_BY(e.lib, e.event_time) AS last_lib,
    MAX_BY(e.os, e.event_time) AS last_os,
    MAX_BY(e.device_type, e.event_time) AS last_device_type,
    MAX_BY(e.country, e.event_time) AS last_country,
    MAX_BY(e.city, e.event_time) AS last_city,
    MAX_BY(e.ip, e.event_time) AS last_ip,
    MAX_BY(
        CASE WHEN e.`type` IN ('profile_set', 'profile_set_once') THEN e.properties END,
        CASE WHEN e.`type` IN ('profile_set', 'profile_set_once') THEN e.event_time END
    ) AS user_properties,
    NOW() AS updated_at
FROM ods.ods_events e
GROUP BY e.project, e.distinct_id;

-- ========== 9.2 事件生命周期清理（外部调度）==========
-- StarRocks SUBMIT TASK 仅支持 INSERT，DELETE 需外部调度执行
-- 按 event_group 差异化保留：
--   CORE  → 365 天（dynamic_partition.start=-365，超期分区自动删除）
--   TRACE → 180 天（外部定时 DELETE 行级清理）
--   DEBUG → 30 天 （外部定时 DELETE 行级清理）
--
-- 执行方式：make lifecycle-cleanup  或 crontab 每日调度
-- 清理 SQL：
--   DELETE FROM ods.ods_events
--   WHERE (event_group = 'DEBUG' AND dt < DATE_SUB(CURDATE(), INTERVAL 30 DAY))
--      OR (event_group = 'TRACE' AND dt < DATE_SUB(CURDATE(), INTERVAL 180 DAY));
--
-- crontab 示例（每天 03:00 执行）：
--   0 3 * * * cd /path/to/Lakehouse-track && make lifecycle-cleanup

-- ========== 10. 用户与 Resource Group 工作负载隔离 ==========
-- 通过 User + Resource Group + exclusive_cpu_cores 实现 ETL/Query CPU 硬隔离
-- CN 支持 --scale 弹性伸缩，Resource Group 自动在所有节点生效
--
-- 隔离模型（每个 CN 节点上）：
--   ┌─────────────────────────────────────────────────┐
--   │  Exclusive Cores (rg_etl)  │  Shared Cores       │
--   │  Pipe INSERT / Broker Load │  rg_query (weight=8) │
--   │  独占 2 核，不受查询影响    │  default_mv_wg (w=2) │
--   │                             │  default_wg (其他)   │
--   └─────────────────────────────────────────────────┘
--
-- 用户分离：
--   root    — Pipe 内部 INSERT、管理操作 → rg_etl / default_wg
--   analyst — 应用层查询（Dashboard/报表/API）→ rg_query

-- ---- 7.1 创建查询专用用户 ----
CREATE USER IF NOT EXISTS 'analyst' IDENTIFIED BY 'analyst';
GRANT SELECT ON ALL TABLES IN DATABASE ods TO 'analyst';
GRANT USAGE ON CATALOG pg_catalog TO 'analyst';

-- ---- 7.2 ETL 资源组（CPU 硬隔离）----
-- exclusive_cpu_cores=2: 独占 2 个核心，Pipe INSERT 不受查询负载影响
-- 开发环境 CPU 核心少时可调为 1；生产环境按 CN 核心数 20~30% 分配
CREATE RESOURCE GROUP IF NOT EXISTS rg_etl
TO (query_type in ('insert'))
WITH (
    'exclusive_cpu_cores' = '2',
    'mem_limit' = '40%',
    'concurrency_limit' = '8'
);

-- ---- 7.3 查询资源组 ----
-- cpu_weight=8: 在 Shared Cores 中获得最高 CPU 份额
-- analyst 用户的 SELECT 自动路由到此资源组
CREATE RESOURCE GROUP IF NOT EXISTS rg_query
TO (user='analyst', query_type in ('select'))
WITH (
    'cpu_weight' = '8',
    'mem_limit' = '60%',
    'concurrency_limit' = '0',
    'big_query_cpu_second_limit' = '300',
    'big_query_scan_rows_limit' = '5000000000',
    'big_query_mem_limit' = '2147483648'
);

-- ---- 7.4 调优 MV 刷新资源组（系统内置）----
ALTER RESOURCE GROUP default_mv_wg WITH (
    'cpu_weight' = '2',
    'mem_limit' = '30%',
    'concurrency_limit' = '3'
);

-- ========== 11. 存算分离状态验证 ==========
-- SHOW STORAGE VOLUMES;
-- DESC STORAGE VOLUME builtin_storage_volume;
-- SHOW PROC '/datacache';
-- SHOW RESOURCE GROUPS ALL;
-- SHOW USAGE RESOURCE GROUPS;
-- SHOW COMPUTE NODES;
--
-- 应用层查询连接：mysql -h 127.0.0.1 -P 9030 -u analyst -panalyst
-- CN 弹性伸缩：docker compose up -d --scale starrocks-cn=N
