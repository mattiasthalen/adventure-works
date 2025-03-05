MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__person__employee),
  references (_hook__department, _hook__reference__shift)
);

SELECT
  'employee_department_histories' AS peripheral,
  _hook__person__employee::BLOB,
  _hook__department::BLOB,
  _hook__reference__shift::BLOB,
  employee_department_history__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  employee_department_history__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  employee_department_history__record_version::TEXT AS bridge__record_version,
  employee_department_history__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  employee_department_history__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  employee_department_history__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__employee_department_histories
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts