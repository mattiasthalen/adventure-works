MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column ship_method__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__ship_method)
FROM silver.bag__adventure_works__ship_methods
WHERE 1 = 1
AND ship_method__record_updated_at BETWEEN @start_ts AND @end_ts