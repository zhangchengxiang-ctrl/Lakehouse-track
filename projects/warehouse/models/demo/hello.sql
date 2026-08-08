MODEL (
  name demo.hello,
  kind FULL,
  cron '@daily',
  grain id,
);

SELECT
  1 AS id,
  'lakehouse-track' AS source,
  CURRENT_TIMESTAMP() AS loaded_at;
