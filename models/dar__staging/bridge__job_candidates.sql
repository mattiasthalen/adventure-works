MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__department, _pit_hook__job_candidate, _pit_hook__person__employee, _pit_hook__reference__shift)
);

WITH cte__bridge AS (
  SELECT
    'job_candidates' AS peripheral,
    _pit_hook__job_candidate,
    _hook__job_candidate,
    _hook__person__employee,
    _hook__person__employee,
    _hook__person__employee,
    job_candidate__record_loaded_at AS bridge__record_loaded_at,
    job_candidate__record_updated_at AS bridge__record_updated_at,
    job_candidate__record_valid_from AS bridge__record_valid_from,
    job_candidate__record_valid_to AS bridge__record_valid_to,
    job_candidate__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__job_candidates
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__job_candidate,
    bridge__employee_department_histories._pit_hook__department,
    bridge__employee_department_histories._pit_hook__person__employee,
    bridge__employee_department_histories._pit_hook__reference__shift,
    bridge__employee_pay_histories._pit_hook__person__employee,
    bridge__employees._pit_hook__person__employee,
    cte__bridge._hook__job_candidate,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__employees.bridge__record_loaded_at,
        bridge__employee_pay_histories.bridge__record_loaded_at,
        bridge__employee_department_histories.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__employees.bridge__record_updated_at,
        bridge__employee_pay_histories.bridge__record_updated_at,
        bridge__employee_department_histories.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__employees.bridge__record_valid_from,
        bridge__employee_pay_histories.bridge__record_valid_from,
        bridge__employee_department_histories.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__employees.bridge__record_valid_to,
        bridge__employee_pay_histories.bridge__record_valid_to,
        bridge__employee_department_histories.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__employees.bridge__is_current_record,
          bridge__employee_pay_histories.bridge__is_current_record,
          bridge__employee_department_histories.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__employees
  ON cte__bridge._hook__person__employee = bridge__employees._hook__person__employee
  AND cte__bridge.bridge__record_valid_from >= bridge__employees.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__employees.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__employee_pay_histories
  ON cte__bridge._hook__person__employee = bridge__employee_pay_histories._hook__person__employee
  AND cte__bridge.bridge__record_valid_from >= bridge__employee_pay_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__employee_pay_histories.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__employee_department_histories
  ON cte__bridge._hook__person__employee = bridge__employee_department_histories._hook__person__employee
  AND cte__bridge.bridge__record_valid_from >= bridge__employee_department_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__employee_department_histories.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__department::TEXT,
      _pit_hook__job_candidate::TEXT,
      _pit_hook__person__employee::TEXT,
      _pit_hook__reference__shift::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__department::BLOB,
  _pit_hook__job_candidate::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__reference__shift::BLOB,
  _hook__job_candidate::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts