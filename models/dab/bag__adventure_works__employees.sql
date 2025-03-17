MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__employee
  ),
  tags hook,
  grain (_pit_hook__person__employee, _hook__person__employee),
  description 'Hook viewpoint of employees data: Employee information.',
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
    employee__record_loaded_at = 'Timestamp when this record was loaded into the system',
    employee__record_updated_at = 'Timestamp when this record was last updated',
    employee__record_version = 'Version number for this record',
    employee__record_valid_from = 'Timestamp from which this record version is valid',
    employee__record_valid_to = 'Timestamp until which this record version is valid',
    employee__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__person__employee = 'Reference hook to employee person',
    _pit_hook__person__employee = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    business_entity_id AS employee__business_entity_id,
    national_idnumber AS employee__national_idnumber,
    login_id AS employee__login_id,
    job_title AS employee__job_title,
    birth_date AS employee__birth_date,
    marital_status AS employee__marital_status,
    gender AS employee__gender,
    hire_date AS employee__hire_date,
    salaried_flag AS employee__salaried_flag,
    vacation_hours AS employee__vacation_hours,
    sick_leave_hours AS employee__sick_leave_hours,
    current_flag AS employee__current_flag,
    rowguid AS employee__rowguid,
    organization_level AS employee__organization_level,
    modified_date AS employee__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS employee__record_loaded_at
  FROM das.raw__adventure_works__employees
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY employee__business_entity_id ORDER BY employee__record_loaded_at) AS employee__record_version,
    CASE
      WHEN employee__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE employee__record_loaded_at
    END AS employee__record_valid_from,
    COALESCE(
      LEAD(employee__record_loaded_at) OVER (PARTITION BY employee__business_entity_id ORDER BY employee__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS employee__record_valid_to,
    employee__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS employee__is_current_record,
    CASE
      WHEN employee__is_current_record
      THEN employee__record_loaded_at
      ELSE employee__record_valid_to
    END AS employee__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('person__employee__adventure_works|', employee__business_entity_id) AS _hook__person__employee,
    CONCAT_WS('~',
      _hook__person__employee,
      'epoch__valid_from|'||employee__record_valid_from
    ) AS _pit_hook__person__employee,
    *
  FROM validity
)
SELECT
  _pit_hook__person__employee::BLOB,
  _hook__person__employee::BLOB,
  employee__business_entity_id::BIGINT,
  employee__national_idnumber::TEXT,
  employee__login_id::TEXT,
  employee__job_title::TEXT,
  employee__birth_date::DATE,
  employee__marital_status::TEXT,
  employee__gender::TEXT,
  employee__hire_date::DATE,
  employee__salaried_flag::BOOLEAN,
  employee__vacation_hours::BIGINT,
  employee__sick_leave_hours::BIGINT,
  employee__current_flag::BOOLEAN,
  employee__rowguid::TEXT,
  employee__organization_level::BIGINT,
  employee__modified_date::DATE,
  employee__record_loaded_at::TIMESTAMP,
  employee__record_updated_at::TIMESTAMP,
  employee__record_version::TEXT,
  employee__record_valid_from::TIMESTAMP,
  employee__record_valid_to::TIMESTAMP,
  employee__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND employee__record_updated_at BETWEEN @start_ts AND @end_ts