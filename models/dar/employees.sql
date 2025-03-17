MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__employee),
  description 'Business viewpoint of employees data: Employee information.',
  column_descriptions (
    employee__business_entity_id = 'Primary key for Employee records. Foreign key to BusinessEntity.BusinessEntityID.',
    employee__national_idnumber = 'Unique national identification number such as a social security number.',
    employee__login_id = 'Network login.',
    employee__job_title = 'Work title such as Buyer or Sales Representative.',
    employee__birth_date = 'Date of birth.',
    employee__marital_status = 'M = Married, S = Single.',
    employee__gender = 'M = Male, F = Female.',
    employee__hire_date = 'Employee hired on this date.',
    employee__salaried_flag = 'Job classification. 0 = Hourly, not exempt from collective bargaining. 1 = Salaried, exempt from collective bargaining.',
    employee__vacation_hours = 'Number of available vacation hours.',
    employee__sick_leave_hours = 'Number of available sick leave hours.',
    employee__current_flag = '0 = Inactive, 1 = Active.',
    employee__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    employee__organization_level = 'The depth of the employee in the corporate hierarchy.',
    employee__modified_date = 'Date when this record was last modified',
    employee__record_loaded_at = 'Timestamp when this record was loaded into the system',
    employee__record_updated_at = 'Timestamp when this record was last updated',
    employee__record_version = 'Version number for this record',
    employee__record_valid_from = 'Timestamp from which this record version is valid',
    employee__record_valid_to = 'Timestamp until which this record version is valid',
    employee__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__person__employee)
FROM dab.bag__adventure_works__employees