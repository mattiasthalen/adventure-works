MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of employee_pay_histories data: Employee pay history.',
  column_descriptions (
    business_entity_id = 'Employee identification number. Foreign key to Employee.BusinessEntityID.',
    rate_change_date = 'Date the change in pay is effective.',
    rate = 'Salary hourly rate.',
    pay_frequency = '1 = Salary received monthly, 2 = Salary received biweekly.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    business_entity_id::BIGINT,
    rate_change_date::DATE,
    rate::DOUBLE,
    pay_frequency::BIGINT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__employee_pay_histories"
)
;