MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__department, _pit_hook__person__employee, _pit_hook__reference__shift)
);

WITH cte__bridge AS (
  SELECT
    'employee_department_histories' AS peripheral,
    _pit_hook__person__employee,
    _hook__person__employee,
    _hook__department,
    _hook__reference__shift,
    _hook__epoch__date,
    measure__employee_department_histories_started,
    measure__employee_department_histories_modified,
    measure__employee_department_histories_finished,
    employee_department_history__record_loaded_at AS bridge__record_loaded_at,
    employee_department_history__record_updated_at AS bridge__record_updated_at,
    employee_department_history__record_valid_from AS bridge__record_valid_from,
    employee_department_history__record_valid_to AS bridge__record_valid_to,
    employee_department_history__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__employee_department_histories
  LEFT JOIN dar__staging.measure__adventure_works__employee_department_histories USING (_pit_hook__person__employee)
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__person__employee,
    uss_bridge__departments._pit_hook__department,
    uss_bridge__shifts._pit_hook__reference__shift,
    cte__bridge._hook__person__employee,
    cte__bridge._hook__epoch__date,
    cte__bridge.measure__employee_department_histories_started,
    cte__bridge.measure__employee_department_histories_modified,
    cte__bridge.measure__employee_department_histories_finished,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__departments.bridge__record_loaded_at,
        uss_bridge__shifts.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__departments.bridge__record_updated_at,
        uss_bridge__shifts.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__departments.bridge__record_valid_from,
        uss_bridge__shifts.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__departments.bridge__record_valid_to,
        uss_bridge__shifts.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record::BOOL,
          uss_bridge__departments.bridge__is_current_record::BOOL,
          uss_bridge__shifts.bridge__is_current_record::BOOL
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.uss_bridge__departments
  ON cte__bridge._hook__department = uss_bridge__departments._hook__department
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__departments.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__departments.bridge__record_valid_to
  LEFT JOIN dar__staging.uss_bridge__shifts
  ON cte__bridge._hook__reference__shift = uss_bridge__shifts._hook__reference__shift
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__shifts.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__shifts.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _hook__epoch__date::TEXT,
      _pit_hook__department::TEXT,
      _pit_hook__person__employee::TEXT,
      _pit_hook__reference__shift::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__department::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__reference__shift::BLOB,
  _hook__person__employee::BLOB,
  _hook__epoch__date::BLOB,
  measure__employee_department_histories_started::INT,
  measure__employee_department_histories_modified::INT,
  measure__employee_department_histories_finished::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts