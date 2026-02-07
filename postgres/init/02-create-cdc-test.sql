-- CDC 测试表（PG CDC -> Paimon）
-- 需 wal_level=logical（docker-compose 已配置）
ALTER USER paimon WITH REPLICATION;

CREATE TABLE IF NOT EXISTS cdc_test_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- REPLICA IDENTITY FULL 使 UPDATE/DELETE 能捕获完整旧值
ALTER TABLE cdc_test_orders REPLICA IDENTITY FULL;

INSERT INTO cdc_test_orders (order_id, user_id, amount) VALUES
    ('o1', 'u1', 99.99),
    ('o2', 'u2', 199.50);
