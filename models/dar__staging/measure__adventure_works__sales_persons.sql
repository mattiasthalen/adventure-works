MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__sales,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__person__sales, _hook__epoch__date),
  references (_pit_hook__person__sales, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__person__sales,
    sales_person__modified_date
  FROM dab.bag__adventure_works__sales_persons
  WHERE 1 = 1
  AND sales_person__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__modified_date AS (
  SELECT
    _pit_hook__person__sales,
    sales_person__modified_date AS measure_date,
    1 AS measure__sales_persons_modified
  FROM cte__source
  WHERE sales_person__modified_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__modified_date
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__person__sales::BLOB,
  _hook__epoch__date::BLOB,
  measure__sales_persons_modified::INT
FROM cte__epoch