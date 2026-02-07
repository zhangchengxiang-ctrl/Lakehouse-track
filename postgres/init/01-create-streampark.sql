-- 创建 StreamPark 元数据库（与 Paimon 共用同一 PostgreSQL 实例）
CREATE DATABASE streampark;
GRANT ALL PRIVILEGES ON DATABASE streampark TO paimon;
