MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__person__employee)
);

SELECT
  'employees' AS peripheral,
  _hook__person__employee::BLOB,
  employee__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  employee__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  employee__record_version::TEXT AS bridge__record_version,
  employee__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  employee__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  employee__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__employees
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts