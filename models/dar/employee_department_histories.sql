MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__employee_department_history),
  description 'Business viewpoint of employee_department_histories data: Employee department transfers.',
  column_descriptions (
    employee_department_history__business_entity_id = 'Employee identification number. Foreign key to Employee.BusinessEntityID.',
    employee_department_history__department_id = 'Department in which the employee worked including currently. Foreign key to Department.DepartmentID.',
    employee_department_history__shift_id = 'Identifies which 8-hour shift the employee works. Foreign key to Shift.Shift.ID.',
    employee_department_history__start_date = 'Date the employee started work in the department.',
    employee_department_history__end_date = 'Date the employee left the department. NULL = Current department.',
    employee_department_history__modified_date = 'Date when this record was last modified',
    employee_department_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    employee_department_history__record_updated_at = 'Timestamp when this record was last updated',
    employee_department_history__record_version = 'Version number for this record',
    employee_department_history__record_valid_from = 'Timestamp from which this record version is valid',
    employee_department_history__record_valid_to = 'Timestamp until which this record version is valid',
    employee_department_history__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__employee_department_history,
    employee_department_history__business_entity_id,
    employee_department_history__department_id,
    employee_department_history__shift_id,
    employee_department_history__start_date,
    employee_department_history__end_date,
    employee_department_history__modified_date,
    employee_department_history__record_loaded_at,
    employee_department_history__record_updated_at,
    employee_department_history__record_version,
    employee_department_history__record_valid_from,
    employee_department_history__record_valid_to,
    employee_department_history__is_current_record
  FROM dab.bag__adventure_works__employee_department_histories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__employee_department_history,
    NULL AS employee_department_history__business_entity_id,
    NULL AS employee_department_history__department_id,
    NULL AS employee_department_history__shift_id,
    NULL AS employee_department_history__start_date,
    NULL AS employee_department_history__end_date,
    NULL AS employee_department_history__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS employee_department_history__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS employee_department_history__record_updated_at,
    0 AS employee_department_history__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS employee_department_history__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS employee_department_history__record_valid_to,
    TRUE AS employee_department_history__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__employee_department_history::BLOB,
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
  employee_department_history__is_current_record::BOOLEAN
FROM cte__final