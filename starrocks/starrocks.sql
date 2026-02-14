-- ============================================================
-- StarRocks 统一初始化：埋点采集 + CDC 双链路（存算分离模式）
--
-- 存储架构：
--   全量数据 → S3 (MinIO lakehouse bucket) 持久化
--   热数据   → CN 本地 DataCache 加速（按 partition_duration 自动分层）
--
-- 埋点链路：Vector → S3 (TSV.gz) → Pipe + FILES() → ods_events / ods_id_mapping
-- CDC 链路：Flink → Paimon → paimon_catalog 外部目录（由 lakehouse.sh 创建）
--           PostgreSQL → pg_catalog JDBC 外部目录（直读）
--
-- 执行：mysql -h 127.0.0.1 -P 9030 -u root < starrocks/starrocks.sql
-- 前置：docker compose up -d --build --scale collection=2
-- ============================================================

-- 全局开启 DataCache 扫描（存算分离核心开关）
SET GLOBAL enable_scan_datacache = true;
-- 开启 DataCache 异步填充（查询完成后异步写入缓存，不阻塞查询返回）
SET GLOBAL enable_populate_datacache = true;
SET new_planner_optimize_timeout = 30000;

-- ========== 1. ODS 数据库 ==========
CREATE DATABASE IF NOT EXISTS ods;
USE ods;

-- ========== 2. ODS 事件内表 ==========
-- 查询模式优化说明：
--   高频查询：WHERE dt BETWEEN ... AND event = '...'
--            WHERE dt = ... AND event IN (...) AND project = '...'
--            WHERE dt = ... AND event_group = 'CORE'
--            WHERE distinct_id = '...' ORDER BY time  (用户旅程)
--
-- 排序键设计 (dt, event, event_group, project)：
--   · dt         — 分区键，几乎所有查询都带日期条件，排第一保证分区裁剪
--   · event      — 中等基数(~数百)，绝大多数分析查询按事件过滤，紧接 dt
--                   使同一事件数据物理连续存储，Zone Map 可高效跳过无关 block
--   · event_group — 极低基数(3值: CORE/TRACE/DEBUG)，常用于过滤噪声数据
--   · project    — 低基数(~数十)，多项目场景下的常用筛选维度
--   注：distinct_id 基数极高(百万级)，放入排序键会破坏 event 的连续性，
--       改由 HASH 分桶 + Bloom Filter 加速点查
--
CREATE TABLE IF NOT EXISTS ods.ods_events (
    -- ---- 排序键列（前 4 列构成 DUPLICATE KEY）----
    dt DATE NOT NULL COMMENT '日期分区键',
    `event` VARCHAR(256) NOT NULL COMMENT '事件名称',
    event_group VARCHAR(16) NOT NULL COMMENT 'CORE/TRACE/DEBUG',
    project VARCHAR(128) COMMENT '项目名称',
    -- ---- 高频查询列 ----
    distinct_id VARCHAR(256) NOT NULL COMMENT '用户标识',
    `time` DATETIME COMMENT '事件时间',
    login_id VARCHAR(256) COMMENT '登录ID',
    anonymous_id VARCHAR(256) COMMENT '匿名ID',
    `type` VARCHAR(64) COMMENT '事件类型',
    -- ---- 属性与上下文 ----
    properties JSON COMMENT '事件属性（JSON）',
    properties_keys VARCHAR(65533) COMMENT '属性键列表',
    original_id VARCHAR(256) COMMENT '原始ID',
    remote_addr VARCHAR(64) COMMENT '客户端 IP',
    `hour` VARCHAR(4) COMMENT '小时',
    -- ---- UA / GeoIP（宽列，查询频率较低）----
    ua_browser VARCHAR(128) COMMENT '浏览器',
    ua_os VARCHAR(128) COMMENT '操作系统',
    ua_device VARCHAR(128) COMMENT '设备类型',
    geoip JSON COMMENT 'GeoIP 信息',
    redis_meta VARCHAR(1024) COMMENT '元数据（预留）',
    -- ---- 二级索引 ----
    INDEX idx_event (`event`) USING BITMAP COMMENT '事件名 bitmap，加速 IN 查询',
    INDEX idx_event_group (event_group) USING BITMAP COMMENT '事件分组 bitmap',
    INDEX idx_project (project) USING BITMAP COMMENT '项目 bitmap',
    INDEX idx_hour (`hour`) USING BITMAP COMMENT '小时 bitmap，时段分析'
)
DUPLICATE KEY (dt, `event`, event_group, project)
PARTITION BY RANGE (dt) ()
DISTRIBUTED BY HASH(distinct_id) BUCKETS 16
PROPERTIES (
    -- ---- 动态分区（建表时关闭，添加历史分区后再开启）----
    "dynamic_partition.enable" = "false",
    "dynamic_partition.time_unit" = "DAY",
    -- 保留全部历史分区 + 预创建未来 3 天分区
    -- 生产 1TB/天 × 365 天 ≈ 365TB S3 存储（存算分离下 S3 成本低）
    -- 注：-9999 表示不自动删除历史分区，生产可按需调小（如 -365）
    "dynamic_partition.start" = "-9999",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    -- ---- 存储与压缩 ----
    "replication_num" = "1",
    -- ZSTD 压缩比比 LZ4 高 30~50%，1TB/天场景节省可观存储
    -- 解压速度仍然足够（~500MB/s），不影响查询延迟
    "compression" = "ZSTD",
    -- ---- 索引 ----
    "bloom_filter_columns" = "distinct_id,login_id",
    -- ---- 存算分离 DataCache 冷热分层 ----
    -- 全量数据持久化在 S3，最近 30 天的分区缓存到 CN 本地磁盘
    -- 热数据（≤30天）：查询直接走本地缓存，延迟 ~ms 级
    -- 冷数据（>30天）：查询走 S3 读取，延迟 ~百ms 级，按需缓存
    "datacache.enable" = "true",
    "datacache.partition_duration" = "30 day"
);

-- 手动创建历史+未来分区，覆盖测试数据完整时间跨度
-- 测试数据事件时间: 2009~2024-05-29，部分 payload 含 2026 未来日期
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20000101
    VALUES [('2000-01-01'), ('2010-01-01'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20100101
    VALUES [('2010-01-01'), ('2011-01-01'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20110101
    VALUES [('2011-01-01'), ('2014-01-01'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20140101
    VALUES [('2014-01-01'), ('2015-01-01'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20150101
    VALUES [('2015-01-01'), ('2024-01-01'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20240101
    VALUES [('2024-01-01'), ('2024-05-27'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20240527
    VALUES [('2024-05-27'), ('2024-05-28'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20240528
    VALUES [('2024-05-28'), ('2024-05-29'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20240529
    VALUES [('2024-05-29'), ('2025-01-01'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20250101
    VALUES [('2025-01-01'), ('2025-07-01'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20250701
    VALUES [('2025-07-01'), ('2026-01-01'));
-- 未来分区由 dynamic_partition.end=3 自动创建当天 +3 天
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20260218
    VALUES [('2026-02-18'), ('2026-07-01'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20260701
    VALUES [('2026-07-01'), ('2027-01-01'));
ALTER TABLE ods.ods_events ADD PARTITION IF NOT EXISTS p20270101
    VALUES [('2027-01-01'), ('2028-01-01'));

-- 开启动态分区（自动管理当天 +3 天范围内的分区，不删除历史分区）
ALTER TABLE ods.ods_events SET ("dynamic_partition.enable" = "true");

-- ========== 3. Pipe：S3 TSV.gz → ods_events（增量摄入） ==========
-- Vector 写入路径：s3://lakehouse/track/events/dt={date}/hour={hour}/{uuid}.tsv.gz
-- TSV 列顺序（tab 分隔）：
--   $1  time           $2  distinct_id    $3  login_id       $4  anonymous_id
--   $5  original_id    $6  event          $7  type           $8  project
--   $9  properties     $10 properties_keys $11 ua_browser     $12 ua_os
--   $13 ua_device      $14 geoip          $15 redis_meta     $16 remote_addr
--   $17 event_group    $18 dt             $19 hour
CREATE PIPE IF NOT EXISTS ods.pipe_s3_events
PROPERTIES (
    "AUTO_INGEST" = "FALSE",
    "POLL_INTERVAL" = "60"
)
AS INSERT INTO ods.ods_events (
    dt, distinct_id, `event`, `time`, login_id, anonymous_id, original_id,
    `type`, project, properties, properties_keys,
    ua_browser, ua_os, ua_device, geoip, redis_meta,
    remote_addr, event_group, `hour`
)
SELECT
    CAST($18 AS DATE),         -- dt
    $2,                         -- distinct_id
    $6,                         -- event
    CAST($1 AS DATETIME),      -- time
    $3,                         -- login_id
    $4,                         -- anonymous_id
    $5,                         -- original_id
    $7,                         -- type
    $8,                         -- project
    CAST($9 AS JSON),          -- properties
    $10,                        -- properties_keys
    $11,                        -- ua_browser
    $12,                        -- ua_os
    $13,                        -- ua_device
    CAST($14 AS JSON),         -- geoip
    $15,                        -- redis_meta
    $16,                        -- remote_addr
    $17,                        -- event_group
    $19                         -- hour
FROM FILES (
    "path" = "s3://lakehouse/track/events/*/*/*.tsv.gz",
    "format" = "csv",
    "csv.column_separator" = "\t",
    "csv.row_delimiter" = "\n",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "minioadmin",
    "aws.s3.secret_key" = "minioadmin",
    "aws.s3.enable_path_style_access" = "true"
)
WHERE $1 != '' AND $18 != ''
  AND $18 >= '2000-01-01' AND $18 <= '2027-12-31';

-- ========== 4. 用户 ID Mapping 内表 + Pipe ==========
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

CREATE PIPE IF NOT EXISTS ods.pipe_s3_id_mapping
PROPERTIES (
    "AUTO_INGEST" = "FALSE",
    "POLL_INTERVAL" = "60"
)
AS INSERT INTO ods.ods_id_mapping (
    project, anonymous_id, login_id, map_type, distinct_id, original_id,
    first_seen, last_seen
)
SELECT
    $1,                         -- project
    $3,                         -- anonymous_id
    $4,                         -- login_id
    $2,                         -- map_type
    $5,                         -- distinct_id
    $6,                         -- original_id
    CAST($7 AS DATETIME),      -- first_seen ← event_time
    CAST($7 AS DATETIME)       -- last_seen  ← event_time
FROM FILES (
    "path" = "s3://lakehouse/track/id_mapping/*/*/*.tsv.gz",
    "format" = "csv",
    "csv.column_separator" = "\t",
    "csv.row_delimiter" = "\n",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "minioadmin",
    "aws.s3.secret_key" = "minioadmin",
    "aws.s3.enable_path_style_access" = "true"
)
WHERE $3 IS NOT NULL AND $3 != ''
  AND $4 IS NOT NULL AND $4 != '';

-- ========== 5. CDC 链路：JDBC 外部目录 ==========
-- 直读 PostgreSQL，用于轻量级维度查询
-- Paimon Catalog 由 scripts/lakehouse.sh run-sql 脚本创建，此处不重复定义
CREATE EXTERNAL CATALOG IF NOT EXISTS pg_catalog
PROPERTIES (
    "type" = "jdbc",
    "user" = "paimon",
    "password" = "paimon123",
    "jdbc_uri" = "jdbc:postgresql://postgres:5432/paimon_db",
    "driver_url" = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar",
    "driver_class" = "org.postgresql.Driver"
);

-- ========== 6. 业务物化视图 ==========
-- 每日事件统计（基于 ods_events，自动刷新）
-- GROUP BY 顺序与排序键 (dt, event, event_group, project) 对齐，
-- 刷新时可利用数据物理有序性加速聚合
CREATE MATERIALIZED VIEW IF NOT EXISTS ods.dws_daily_event_stats
REFRESH ASYNC EVERY (INTERVAL 10 MINUTE)
AS
SELECT
    e.dt,
    e.`event`,
    e.event_group,
    e.project,
    COUNT(*) AS event_count,
    COUNT(DISTINCT e.distinct_id) AS uv
FROM ods.ods_events e
GROUP BY e.dt, e.`event`, e.event_group, e.project;

-- ========== 7. 存算分离状态验证 ==========
-- 执行后应看到 built-in Storage Volume 指向 s3://lakehouse
-- SHOW STORAGE VOLUMES;
-- DESC STORAGE VOLUME builtin_storage_volume;
-- 查看 DataCache 命中率：
-- SHOW PROC '/datacache';
