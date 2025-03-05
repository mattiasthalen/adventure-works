MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__person__employee)
);

SELECT
  'employee_pay_histories' AS peripheral,
  _hook__person__employee::BLOB,
  employee_pay_history__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  employee_pay_history__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  employee_pay_history__record_version::TEXT AS bridge__record_version,
  employee_pay_history__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  employee_pay_history__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  employee_pay_history__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__employee_pay_histories
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts