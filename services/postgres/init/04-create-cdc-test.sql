-- CDC 测试表（PG CDC → Paimon）
-- 独立 schema cdc_test，与业务 schema stellar_trace 隔离
-- 需 wal_level=logical（docker-compose 已配置）

ALTER USER postgres WITH REPLICATION;

CREATE SCHEMA IF NOT EXISTS cdc_test;
SET search_path TO cdc_test;

CREATE TABLE IF NOT EXISTS cdc_test_orders (
    order_id   VARCHAR(64)    PRIMARY KEY,          -- 订单 ID
    user_id    VARCHAR(64)    NOT NULL,              -- 用户 ID
    amount     DECIMAL(10,2),                        -- 金额
    created_at TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- REPLICA IDENTITY FULL 使 UPDATE/DELETE 能捕获完整旧值
ALTER TABLE cdc_test_orders REPLICA IDENTITY FULL;

-- 测试种子数据
INSERT INTO cdc_test_orders (order_id, user_id, amount) VALUES
    ('o1', 'u1', 99.99),
    ('o2', 'u2', 199.50)
ON CONFLICT (order_id) DO NOTHING;
