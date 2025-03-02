MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column territory__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__territory)
FROM silver.bag__adventure_works__sales_territories
WHERE 1 = 1
AND territory__record_updated_at BETWEEN @start_ts AND @end_ts