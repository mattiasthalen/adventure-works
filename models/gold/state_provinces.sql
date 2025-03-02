MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column state_province__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__state_province, _hook__territory)
FROM silver.bag__adventure_works__state_provinces
WHERE 1 = 1
AND state_province__record_updated_at BETWEEN @start_ts AND @end_ts