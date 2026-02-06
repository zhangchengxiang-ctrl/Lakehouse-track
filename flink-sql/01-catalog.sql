-- Paimon JDBC Catalog（Postgres 元数据）
CREATE CATALOG paimon_jdbc WITH (
    'type' = 'paimon',
    'metastore' = 'jdbc',
    'uri' = 'jdbc:postgresql://postgres:5432/paimon_db',
    'user' = 'paimon',
    'password' = 'paimon123',
    'warehouse' = 's3://paimon-lake/data/',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key' = 'minioadmin',
    's3.secret-key' = 'minioadmin',
    's3.path.style.access' = 'true',
    'jdbc.driver' = 'org.postgresql.Driver'
);

USE CATALOG paimon_jdbc;
