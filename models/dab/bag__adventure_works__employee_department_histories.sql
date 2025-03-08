MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__employee,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__person__employee, _hook__person__employee),
  references (_hook__department, _hook__reference__shift)
);

WITH staging AS (
  SELECT
    business_entity_id AS employee_department_history__business_entity_id,
    department_id AS employee_department_history__department_id,
    shift_id AS employee_department_history__shift_id,
    start_date AS employee_department_history__start_date,
    end_date AS employee_department_history__end_date,
    modified_date AS employee_department_history__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS employee_department_history__record_loaded_at
  FROM das.raw__adventure_works__employee_department_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY employee_department_history__business_entity_id ORDER BY employee_department_history__record_loaded_at) AS employee_department_history__record_version,
    CASE
      WHEN employee_department_history__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE employee_department_history__record_loaded_at
    END AS employee_department_history__record_valid_from,
    COALESCE(
      LEAD(employee_department_history__record_loaded_at) OVER (PARTITION BY employee_department_history__business_entity_id ORDER BY employee_department_history__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS employee_department_history__record_valid_to,
    employee_department_history__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS employee_department_history__is_current_record,
    CASE
      WHEN employee_department_history__is_current_record
      THEN employee_department_history__record_loaded_at
      ELSE employee_department_history__record_valid_to
    END AS employee_department_history__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'person__employee__adventure_works|',
      employee_department_history__business_entity_id,
      '~epoch|valid_from|',
      employee_department_history__record_valid_from
    )::BLOB AS _pit_hook__person__employee,
    CONCAT('person__employee__adventure_works|', employee_department_history__business_entity_id) AS _hook__person__employee,
    CONCAT('department__adventure_works|', employee_department_history__department_id) AS _hook__department,
    CONCAT('reference__shift__adventure_works|', employee_department_history__shift_id) AS _hook__reference__shift,
    *
  FROM validity
)
SELECT
  _pit_hook__person__employee::BLOB,
  _hook__person__employee::BLOB,
  _hook__department::BLOB,
  _hook__reference__shift::BLOB,
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
  employee_department_history__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND employee_department_history__record_updated_at BETWEEN @start_ts AND @end_ts