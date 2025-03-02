MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_category__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__product_category)
FROM silver.bag__adventure_works__product_categories
WHERE 1 = 1
AND product_category__record_updated_at BETWEEN @start_ts AND @end_ts