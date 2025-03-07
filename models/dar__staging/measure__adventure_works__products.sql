MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__product,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__product, _hook__epoch__date),
  references (_pit_hook__product, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__product,
    product__sell_start_date,
    product__modified_date,
    product__sell_end_date
  FROM dab.bag__adventure_works__products
  WHERE 1 = 1
  AND product__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__sell_start_date AS (
  SELECT
    _pit_hook__product,
    product__sell_start_date::DATE AS measure_date,
    1 AS measure__products_sell_start
  FROM cte__source
  WHERE product__sell_start_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__product,
    product__modified_date::DATE AS measure_date,
    1 AS measure__products_modified
  FROM cte__source
  WHERE product__modified_date IS NOT NULL
), cte__sell_end_date AS (
  SELECT
    _pit_hook__product,
    product__sell_end_date::DATE AS measure_date,
    1 AS measure__products_sell_end
  FROM cte__source
  WHERE product__sell_end_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__sell_start_date
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__product, measure_date)
  FULL OUTER JOIN cte__sell_end_date USING (_pit_hook__product, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__product::BLOB,
  _hook__epoch__date::BLOB,
  measure__products_sell_start::INT,
  measure__products_modified::INT,
  measure__products_sell_end::INT
FROM cte__epoch