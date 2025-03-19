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

WITH cte__source AS (
  SELECT
    _pit_hook__person__employee,
    employee__business_entity_id,
    employee__national_idnumber,
    employee__login_id,
    employee__job_title,
    employee__birth_date,
    employee__marital_status,
    employee__gender,
    employee__hire_date,
    employee__salaried_flag,
    employee__vacation_hours,
    employee__sick_leave_hours,
    employee__current_flag,
    employee__rowguid,
    employee__organization_level,
    employee__modified_date,
    employee__record_loaded_at,
    employee__record_updated_at,
    employee__record_version,
    employee__record_valid_from,
    employee__record_valid_to,
    employee__is_current_record
  FROM dab.bag__adventure_works__employees
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__person__employee,
    NULL AS employee__business_entity_id,
    'N/A' AS employee__national_idnumber,
    'N/A' AS employee__login_id,
    'N/A' AS employee__job_title,
    NULL AS employee__birth_date,
    'N/A' AS employee__marital_status,
    'N/A' AS employee__gender,
    NULL AS employee__hire_date,
    FALSE AS employee__salaried_flag,
    NULL AS employee__vacation_hours,
    NULL AS employee__sick_leave_hours,
    FALSE AS employee__current_flag,
    'N/A' AS employee__rowguid,
    NULL AS employee__organization_level,
    NULL AS employee__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS employee__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS employee__record_updated_at,
    0 AS employee__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS employee__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS employee__record_valid_to,
    TRUE AS employee__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__person__employee::BLOB,
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
  employee__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.employees TO './export/dar/employees.parquet' (FORMAT parquet, COMPRESSION zstd)
);