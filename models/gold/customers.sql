MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column customer__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__customer, _hook__person, _hook__store, _hook__territory)
FROM silver.bag__adventure_works__customers
WHERE 1 = 1
AND customer__record_updated_at BETWEEN @start_ts AND @end_ts