MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of employees data: Employee information.',
  column_descriptions (
    business_entity_id = 'Primary key for Employee records. Foreign key to BusinessEntity.BusinessEntityID.',
    national_idnumber = 'Unique national identification number such as a social security number.',
    login_id = 'Network login.',
    job_title = 'Work title such as Buyer or Sales Representative.',
    birth_date = 'Date of birth.',
    marital_status = 'M = Married, S = Single.',
    gender = 'M = Male, F = Female.',
    hire_date = 'Employee hired on this date.',
    salaried_flag = 'Job classification. 0 = Hourly, not exempt from collective bargaining. 1 = Salaried, exempt from collective bargaining.',
    vacation_hours = 'Number of available vacation hours.',
    sick_leave_hours = 'Number of available sick leave hours.',
    current_flag = '0 = Inactive, 1 = Active.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    organization_level = 'The depth of the employee in the corporate hierarchy.'
  )
);

SELECT
    business_entity_id::BIGINT,
    national_idnumber::TEXT,
    login_id::TEXT,
    job_title::TEXT,
    birth_date::DATE,
    marital_status::TEXT,
    gender::TEXT,
    hire_date::DATE,
    salaried_flag::BOOLEAN,
    vacation_hours::BIGINT,
    sick_leave_hours::BIGINT,
    current_flag::BOOLEAN,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    organization_level::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__employees"
)
;