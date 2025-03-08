MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order__work,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__order__work, _hook__epoch__date),
  references (_pit_hook__order__work, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__order__work,
    work_order__start_date,
    work_order__end_date,
    work_order__due_date,
    work_order__modified_date
  FROM dab.bag__adventure_works__work_orders
  WHERE 1 = 1
  AND work_order__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__start_date AS (
  SELECT
    _pit_hook__order__work,
    work_order__start_date AS measure_date,
    1 AS measure__work_orders_started
  FROM cte__source
  WHERE work_order__start_date IS NOT NULL
), cte__end_date AS (
  SELECT
    _pit_hook__order__work,
    work_order__end_date AS measure_date,
    1 AS measure__work_orders_finished
  FROM cte__source
  WHERE work_order__end_date IS NOT NULL
), cte__due_date AS (
  SELECT
    _pit_hook__order__work,
    work_order__due_date AS measure_date,
    1 AS measure__work_orders_due
  FROM cte__source
  WHERE work_order__due_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__order__work,
    work_order__modified_date AS measure_date,
    1 AS measure__work_orders_modified
  FROM cte__source
  WHERE work_order__modified_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__start_date
  FULL OUTER JOIN cte__end_date USING (_pit_hook__order__work, measure_date)
  FULL OUTER JOIN cte__due_date USING (_pit_hook__order__work, measure_date)
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__order__work, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__order__work::BLOB,
  _hook__epoch__date::BLOB,
  measure__work_orders_started::INT,
  measure__work_orders_finished::INT,
  measure__work_orders_due::INT,
  measure__work_orders_modified::INT
FROM cte__epoch