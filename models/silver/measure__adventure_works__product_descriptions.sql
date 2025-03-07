MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__product_description,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__reference__product_description, _hook__epoch__date),
  references (_pit_hook__reference__product_description, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__product_description,
    product_description__modified_date
  FROM silver.bag__adventure_works__product_descriptions
  WHERE 1 = 1
  AND product_description__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__modified_date AS (
  SELECT
    _pit_hook__reference__product_description,
    product_description__modified_date::DATE AS measure_date,
    1 AS measure__product_descriptions_modified
  FROM cte__source
  WHERE product_description__modified_date IS NOT NULL
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
  _pit_hook__reference__product_description::BLOB,
  _hook__epoch__date::BLOB,
  measure__product_descriptions_modified::INT
FROM cte__epoch