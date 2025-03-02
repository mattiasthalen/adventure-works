MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_subcategory__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__product_category, _hook__product_subcategory)
FROM silver.bag__adventure_works__product_subcategories
WHERE 1 = 1
AND product_subcategory__record_updated_at BETWEEN @start_ts AND @end_ts