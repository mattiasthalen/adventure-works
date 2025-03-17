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

SELECT
  *
  EXCLUDE (_hook__employee_department_history, _hook__person__employee, _hook__department, _hook__reference__shift, _hook__epoch__start_date)
FROM dab.bag__adventure_works__employee_department_histories