-- Flink CDC: PG orders -> StarRocks dim_orders
-- 需根据实际 PG 连接信息修改 hostname、database-name、table-name 等

CREATE TABLE cdc_orders (
    order_id STRING,
    user_id STRING,
    amount DECIMAL(10,2),
    created_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'pg_user',
    'password' = 'pg_pass',
    'database-name' = 'business_db',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE sr_dim_orders (
    order_id STRING,
    user_id STRING,
    amount DECIMAL(10,2),
    created_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks:9030',
    'load-url' = 'starrocks:8030',
    'database-name' = 'ods',
    'table-name' = 'dim_orders',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'true'
);

INSERT INTO sr_dim_orders SELECT * FROM cdc_orders;
