-- StarRocks PIPE 定义（从 S3 增量摄入）
-- PIPE 依赖 S3 路径中存在 .tsv.gz 数据文件才能创建
-- 首次 reset 时由 lakehouse.sh 延迟重试创建
-- 注意：S3 凭证为 Docker 内部默认值，与 .env 中 S3_* 一致

-- ========== events PIPE ==========
-- Vector 写入路径：s3://lakehouse/track/events/dt={date}/hour={hour}/{uuid}.tsv.gz
-- TSV 列顺序（tab 分隔，11 列）：
--   $1  event_time     $2  distinct_id    $3  login_id       $4  anonymous_id
--   $5  original_id    $6  event          $7  type           $8  project
--   $9  properties     $10 event_group    $11 dt
CREATE PIPE IF NOT EXISTS ods.pipe_s3_events
PROPERTIES (
    "AUTO_INGEST" = "TRUE",
    "POLL_INTERVAL" = "30",
    "BATCH_SIZE" = "4294967296",
    "BATCH_FILES" = "256"
)
AS INSERT INTO ods.ods_events (
    dt, `event`, event_group, project,
    distinct_id, login_id, anonymous_id, original_id, `type`,
    event_time, properties
)
SELECT
    CAST($11 AS DATE),         -- dt
    $6,                         -- event
    $10,                        -- event_group
    $8,                         -- project
    $2,                         -- distinct_id
    $3,                         -- login_id
    $4,                         -- anonymous_id
    $5,                         -- original_id
    $7,                         -- type
    CAST($1 AS DATETIME),      -- event_time
    CAST($9 AS JSON)           -- properties
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
WHERE $1 != '' AND $11 != ''
  AND $11 >= '2000-01-01' AND $11 <= '2027-12-31';

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
