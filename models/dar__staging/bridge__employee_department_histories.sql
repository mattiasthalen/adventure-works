MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__department, _pit_hook__employee_department_history, _pit_hook__person__employee, _pit_hook__reference__shift)
);

WITH cte__bridge AS (
  SELECT
    'employee_department_histories' AS peripheral,
    _pit_hook__employee_department_history,
    _hook__employee_department_history,
    _hook__person__employee,
    _hook__department,
    _hook__reference__shift,
    employee_department_history__record_loaded_at AS bridge__record_loaded_at,
    employee_department_history__record_updated_at AS bridge__record_updated_at,
    employee_department_history__record_valid_from AS bridge__record_valid_from,
    employee_department_history__record_valid_to AS bridge__record_valid_to,
    employee_department_history__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__employee_department_histories
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__employee_department_history,
    bridge__departments._pit_hook__department,
    bridge__employees._pit_hook__person__employee,
    bridge__shifts._pit_hook__reference__shift,
    cte__bridge._hook__employee_department_history,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__employees.bridge__record_loaded_at,
        bridge__departments.bridge__record_loaded_at,
        bridge__shifts.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__employees.bridge__record_updated_at,
        bridge__departments.bridge__record_updated_at,
        bridge__shifts.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__employees.bridge__record_valid_from,
        bridge__departments.bridge__record_valid_from,
        bridge__shifts.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__employees.bridge__record_valid_to,
        bridge__departments.bridge__record_valid_to,
        bridge__shifts.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__employees.bridge__is_current_record,
          bridge__departments.bridge__is_current_record,
          bridge__shifts.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__employees
  ON cte__bridge._hook__person__employee = bridge__employees._hook__person__employee
  AND cte__bridge.bridge__record_valid_from >= bridge__employees.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__employees.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__departments
  ON cte__bridge._hook__department = bridge__departments._hook__department
  AND cte__bridge.bridge__record_valid_from >= bridge__departments.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__departments.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__shifts
  ON cte__bridge._hook__reference__shift = bridge__shifts._hook__reference__shift
  AND cte__bridge.bridge__record_valid_from >= bridge__shifts.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__shifts.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__department::TEXT,
      _pit_hook__employee_department_history::TEXT,
      _pit_hook__person__employee::TEXT,
      _pit_hook__reference__shift::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__department::BLOB,
  _pit_hook__employee_department_history::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__reference__shift::BLOB,
  _hook__employee_department_history::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts