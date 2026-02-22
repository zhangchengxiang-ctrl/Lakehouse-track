-- StarRocks PIPE 定义（从 S3 增量摄入）
-- PIPE 依赖 S3 路径中存在 .tsv.gz 数据文件才能创建
-- 首次 reset 时由 lakehouse.sh 延迟重试创建

-- ========== events PIPE ==========
-- Vector 写入路径：s3://lakehouse/track/events/dt={date}/hour={hour}/{uuid}.tsv.gz
-- TSV 列顺序（tab 分隔）：
--   $1  time           $2  distinct_id    $3  login_id       $4  anonymous_id
--   $5  original_id    $6  event          $7  type           $8  project
--   $9  properties     $10 properties_keys $11 ua_browser     $12 ua_os
--   $13 ua_device      $14 geoip          $15 redis_meta     $16 remote_addr
--   $17 event_group    $18 dt             $19 hour
CREATE PIPE IF NOT EXISTS ods.pipe_s3_events
PROPERTIES (
    "AUTO_INGEST" = "TRUE",
    "POLL_INTERVAL" = "30",
    "BATCH_SIZE" = "4294967296",
    "BATCH_FILES" = "256"
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

-- ========== id_mapping PIPE ==========
-- Vector 写入路径：s3://lakehouse/track/id_mapping/dt={date}/hour={hour}/{uuid}.tsv.gz
-- TSV 列顺序：$1 project | $2 map_type | $3 anonymous_id | $4 login_id |
--             $5 distinct_id | $6 original_id | $7 event_time | $8 dt | $9 hour
CREATE PIPE IF NOT EXISTS ods.pipe_s3_id_mapping
PROPERTIES (
    "AUTO_INGEST" = "TRUE",
    "POLL_INTERVAL" = "30",
    "BATCH_SIZE" = "1073741824",
    "BATCH_FILES" = "128"
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
