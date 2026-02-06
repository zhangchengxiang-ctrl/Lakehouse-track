-- StarRocks 挂载 Paimon

CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "jdbc",
    "paimon.catalog.jdbc.url" = "jdbc:postgresql://postgres:5432/paimon_db",
    "paimon.catalog.jdbc.user" = "paimon",
    "paimon.catalog.jdbc.password" = "paimon123",
    "paimon.catalog.warehouse" = "s3://paimon-lake/data/",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "minioadmin",
    "aws.s3.secret_key" = "minioadmin",
    "aws.s3.enable_path_style_access" = "true"
);
