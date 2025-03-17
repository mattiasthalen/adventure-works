MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of employee_department_histories data: Employee department transfers.',
  column_descriptions (
    business_entity_id = 'Employee identification number. Foreign key to Employee.BusinessEntityID.',
    department_id = 'Department in which the employee worked including currently. Foreign key to Department.DepartmentID.',
    shift_id = 'Identifies which 8-hour shift the employee works. Foreign key to Shift.Shift.ID.',
    start_date = 'Date the employee started work in the department.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    end_date = 'Date the employee left the department. NULL = Current department.'
  )
);

SELECT
    business_entity_id::BIGINT,
    department_id::BIGINT,
    shift_id::BIGINT,
    start_date::DATE,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    end_date::DATE
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__employee_department_histories"
)
;