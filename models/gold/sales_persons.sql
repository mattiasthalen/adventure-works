MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column sales_person__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__sales_person, _hook__territory)
FROM silver.bag__adventure_works__sales_persons
WHERE 1 = 1
AND sales_person__record_updated_at BETWEEN @start_ts AND @end_ts