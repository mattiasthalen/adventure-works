MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__product, _hook__product_model, _hook__product_subcategory)
FROM silver.bag__adventure_works__products
WHERE 1 = 1
AND product__record_updated_at BETWEEN @start_ts AND @end_ts