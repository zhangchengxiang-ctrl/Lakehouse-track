-- Flink CDC: PG orders -> StarRocks dim_orders（示例，需根据实际业务修改）
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
    'jdbc-url' = 'jdbc:mysql://starrocks-fe:9030',
    'load-url' = 'starrocks-fe:8030',
    'database-name' = 'ods',
    'table-name' = 'dim_orders',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'true'
);

INSERT INTO sr_dim_orders SELECT * FROM cdc_orders;
