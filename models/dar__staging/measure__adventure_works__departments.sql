MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__department,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__department, _hook__epoch__date),
  references (_pit_hook__department, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__department,
    department__modified_date
  FROM dab.bag__adventure_works__departments
  WHERE 1 = 1
  AND department__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__modified_date AS (
  SELECT
    _pit_hook__department,
    department__modified_date AS measure_date,
    1 AS measure__departments_modified
  FROM cte__source
  WHERE department__modified_date IS NOT NULL
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
  _pit_hook__department::BLOB,
  _hook__epoch__date::BLOB,
  measure__departments_modified::INT
FROM cte__epoch