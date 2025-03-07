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
    product_list_price_history__start_date,
    product_list_price_history__end_date,
    product_list_price_history__modified_date
  FROM dab.bag__adventure_works__product_list_price_histories
  WHERE 1 = 1
  AND product_list_price_history__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__start_date AS (
  SELECT
    _pit_hook__product,
    product_list_price_history__start_date::DATE AS measure_date,
    1 AS measure__product_list_price_histories_started
  FROM cte__source
  WHERE product_list_price_history__start_date IS NOT NULL
), cte__end_date AS (
  SELECT
    _pit_hook__product,
    product_list_price_history__end_date::DATE AS measure_date,
    1 AS measure__product_list_price_histories_finished
  FROM cte__source
  WHERE product_list_price_history__end_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__product,
    product_list_price_history__modified_date::DATE AS measure_date,
    1 AS measure__product_list_price_histories_modified
  FROM cte__source
  WHERE product_list_price_history__modified_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__start_date
  FULL OUTER JOIN cte__end_date USING (_pit_hook__product, measure_date)
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__product, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__product::BLOB,
  _hook__epoch__date::BLOB,
  measure__product_list_price_histories_started::INT,
  measure__product_list_price_histories_finished::INT,
  measure__product_list_price_histories_modified::INT
FROM cte__epoch