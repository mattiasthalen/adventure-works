MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column address__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__address, _hook__state_province)
FROM silver.bag__adventure_works__addresses
WHERE 1 = 1
AND address__record_updated_at BETWEEN @start_ts AND @end_ts