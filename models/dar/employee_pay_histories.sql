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

WITH cte__source AS (
  SELECT
    _pit_hook__employee_pay_history,
    employee_pay_history__business_entity_id,
    employee_pay_history__rate_change_date,
    employee_pay_history__rate,
    employee_pay_history__pay_frequency,
    employee_pay_history__modified_date,
    employee_pay_history__record_loaded_at,
    employee_pay_history__record_updated_at,
    employee_pay_history__record_version,
    employee_pay_history__record_valid_from,
    employee_pay_history__record_valid_to,
    employee_pay_history__is_current_record
  FROM dab.bag__adventure_works__employee_pay_histories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__employee_pay_history,
    NULL AS employee_pay_history__business_entity_id,
    NULL AS employee_pay_history__rate_change_date,
    NULL AS employee_pay_history__rate,
    NULL AS employee_pay_history__pay_frequency,
    NULL AS employee_pay_history__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS employee_pay_history__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS employee_pay_history__record_updated_at,
    0 AS employee_pay_history__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS employee_pay_history__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS employee_pay_history__record_valid_to,
    TRUE AS employee_pay_history__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__employee_pay_history::BLOB,
  employee_pay_history__business_entity_id::BIGINT,
  employee_pay_history__rate_change_date::DATE,
  employee_pay_history__rate::DOUBLE,
  employee_pay_history__pay_frequency::BIGINT,
  employee_pay_history__modified_date::DATE,
  employee_pay_history__record_loaded_at::TIMESTAMP,
  employee_pay_history__record_updated_at::TIMESTAMP,
  employee_pay_history__record_version::TEXT,
  employee_pay_history__record_valid_from::TIMESTAMP,
  employee_pay_history__record_valid_to::TIMESTAMP,
  employee_pay_history__is_current_record::BOOLEAN
FROM cte__final