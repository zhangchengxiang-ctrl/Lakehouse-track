-- 确保在 Postgres 启动时自动创建 HMS 所需的数据库
SELECT 'CREATE DATABASE metastore'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metastore')\gexec

-- 授予权限
GRANT ALL PRIVILEGES ON DATABASE metastore TO paimon;
