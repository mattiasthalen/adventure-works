MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order_line__purchase,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__order_line__purchase, _hook__epoch__date),
  references (_pit_hook__order_line__purchase, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__order_line__purchase,
    purchase_order_detail__due_date,
    purchase_order_detail__modified_date
  FROM silver.bag__adventure_works__purchase_order_details
  WHERE 1 = 1
  AND purchase_order_detail__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__due_date AS (
  SELECT
    _pit_hook__order_line__purchase,
    purchase_order_detail__due_date::DATE AS measure_date,
    1 AS measure__purchase_order_details_due
  FROM cte__source
  WHERE purchase_order_detail__due_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__order_line__purchase,
    purchase_order_detail__modified_date::DATE AS measure_date,
    1 AS measure__purchase_order_details_modified
  FROM cte__source
  WHERE purchase_order_detail__modified_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__due_date
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__order_line__purchase, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__order_line__purchase::BLOB,
  _hook__epoch__date::BLOB,
  measure__purchase_order_details_due::INT,
  measure__purchase_order_details_modified::INT
FROM cte__epoch