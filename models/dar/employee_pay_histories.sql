MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__employee_pay_history),
  description 'Business viewpoint of employee_pay_histories data: Employee pay history.',
  column_descriptions (
    employee_pay_history__business_entity_id = 'Employee identification number. Foreign key to Employee.BusinessEntityID.',
    employee_pay_history__rate_change_date = 'Date the change in pay is effective.',
    employee_pay_history__rate = 'Salary hourly rate.',
    employee_pay_history__pay_frequency = '1 = Salary received monthly, 2 = Salary received biweekly.',
    employee_pay_history__modified_date = 'Date when this record was last modified',
    employee_pay_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    employee_pay_history__record_updated_at = 'Timestamp when this record was last updated',
    employee_pay_history__record_version = 'Version number for this record',
    employee_pay_history__record_valid_from = 'Timestamp from which this record version is valid',
    employee_pay_history__record_valid_to = 'Timestamp until which this record version is valid',
    employee_pay_history__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__employee_pay_history, _hook__person__employee, _hook__epoch__rate_change_date)
FROM dab.bag__adventure_works__employee_pay_histories