MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__employee_department_history
  ),
  tags hook,
  grain (_pit_hook__employee_department_history, _hook__employee_department_history),
  description 'Hook viewpoint of employee_department_histories data: Employee department transfers.',
  references (_hook__person__employee, _hook__department, _hook__reference__shift, _hook__epoch__start_date),
  column_descriptions (
    employee_department_history__business_entity_id = 'Employee identification number. Foreign key to Employee.BusinessEntityID.',
    employee_department_history__department_id = 'Department in which the employee worked including currently. Foreign key to Department.DepartmentID.',
    employee_department_history__shift_id = 'Identifies which 8-hour shift the employee works. Foreign key to Shift.Shift.ID.',
    employee_department_history__start_date = 'Date the employee started work in the department.',
    employee_department_history__end_date = 'Date the employee left the department. NULL = Current department.',
    employee_department_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    employee_department_history__record_updated_at = 'Timestamp when this record was last updated',
    employee_department_history__record_version = 'Version number for this record',
    employee_department_history__record_valid_from = 'Timestamp from which this record version is valid',
    employee_department_history__record_valid_to = 'Timestamp until which this record version is valid',
    employee_department_history__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__employee_department_history = 'Reference hook to employee_department_history',
    _hook__person__employee = 'Reference hook to employee person',
    _hook__department = 'Reference hook to department',
    _hook__reference__shift = 'Reference hook to shift reference',
    _hook__epoch__start_date = 'Reference hook to start_date epoch',
    _pit_hook__employee_department_history = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    business_entity_id AS employee_department_history__business_entity_id,
    department_id AS employee_department_history__department_id,
    shift_id AS employee_department_history__shift_id,
    start_date AS employee_department_history__start_date,
    end_date AS employee_department_history__end_date,
    modified_date AS employee_department_history__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS employee_department_history__record_loaded_at
  FROM das.raw__adventure_works__employee_department_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY employee_department_history__business_entity_id, employee_department_history__department_id, employee_department_history__shift_id, employee_department_history__start_date ORDER BY employee_department_history__record_loaded_at) AS employee_department_history__record_version,
    CASE
      WHEN employee_department_history__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE employee_department_history__record_loaded_at
    END AS employee_department_history__record_valid_from,
    COALESCE(
      LEAD(employee_department_history__record_loaded_at) OVER (PARTITION BY employee_department_history__business_entity_id, employee_department_history__department_id, employee_department_history__shift_id, employee_department_history__start_date ORDER BY employee_department_history__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS employee_department_history__record_valid_to,
    employee_department_history__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS employee_department_history__is_current_record,
    CASE
      WHEN employee_department_history__is_current_record
      THEN employee_department_history__record_loaded_at
      ELSE employee_department_history__record_valid_to
    END AS employee_department_history__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('person__employee__adventure_works|', employee_department_history__business_entity_id) AS _hook__person__employee,
    CONCAT('department__adventure_works|', employee_department_history__department_id) AS _hook__department,
    CONCAT('reference__shift__adventure_works|', employee_department_history__shift_id) AS _hook__reference__shift,
    CONCAT('epoch__date|', employee_department_history__start_date) AS _hook__epoch__start_date,
    CONCAT_WS('~', _hook__person__employee, _hook__department, _hook__reference__shift, _hook__epoch__start_date) AS _hook__employee_department_history,
    CONCAT_WS('~',
      _hook__employee_department_history,
      'epoch__valid_from|'||employee_department_history__record_valid_from
    ) AS _pit_hook__employee_department_history,
    *
  FROM validity
)
SELECT
  _pit_hook__employee_department_history::BLOB,
  _hook__employee_department_history::BLOB,
  _hook__person__employee::BLOB,
  _hook__department::BLOB,
  _hook__reference__shift::BLOB,
  _hook__epoch__start_date::BLOB,
  employee_department_history__business_entity_id::BIGINT,
  employee_department_history__department_id::BIGINT,
  employee_department_history__shift_id::BIGINT,
  employee_department_history__start_date::DATE,
  employee_department_history__end_date::DATE,
  employee_department_history__modified_date::DATE,
  employee_department_history__record_loaded_at::TIMESTAMP,
  employee_department_history__record_updated_at::TIMESTAMP,
  employee_department_history__record_version::TEXT,
  employee_department_history__record_valid_from::TIMESTAMP,
  employee_department_history__record_valid_to::TIMESTAMP,
  employee_department_history__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND employee_department_history__record_updated_at BETWEEN @start_ts AND @end_ts