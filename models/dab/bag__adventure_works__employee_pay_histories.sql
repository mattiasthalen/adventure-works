MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__employee_pay_history
  ),
  tags hook,
  grain (_pit_hook__employee_pay_history, _hook__employee_pay_history),
  references (_hook__person__employee, _hook__epoch__rate_change_date)
);

WITH staging AS (
  SELECT
    business_entity_id AS employee_pay_history__business_entity_id,
    rate_change_date AS employee_pay_history__rate_change_date,
    rate AS employee_pay_history__rate,
    pay_frequency AS employee_pay_history__pay_frequency,
    modified_date AS employee_pay_history__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS employee_pay_history__record_loaded_at
  FROM das.raw__adventure_works__employee_pay_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY employee_pay_history__business_entity_id, employee_pay_history__rate_change_date ORDER BY employee_pay_history__record_loaded_at) AS employee_pay_history__record_version,
    CASE
      WHEN employee_pay_history__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE employee_pay_history__record_loaded_at
    END AS employee_pay_history__record_valid_from,
    COALESCE(
      LEAD(employee_pay_history__record_loaded_at) OVER (PARTITION BY employee_pay_history__business_entity_id, employee_pay_history__rate_change_date ORDER BY employee_pay_history__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS employee_pay_history__record_valid_to,
    employee_pay_history__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS employee_pay_history__is_current_record,
    CASE
      WHEN employee_pay_history__is_current_record
      THEN employee_pay_history__record_loaded_at
      ELSE employee_pay_history__record_valid_to
    END AS employee_pay_history__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('person__employee__adventure_works|', employee_pay_history__business_entity_id) AS _hook__person__employee,
    CONCAT('epoch__date|', employee_pay_history__rate_change_date) AS _hook__epoch__rate_change_date,
    CONCAT_WS('~', _hook__person__employee, _hook__epoch__rate_change_date) AS _hook__employee_pay_history,
    CONCAT_WS('~',
      _hook__employee_pay_history,
      'epoch__valid_from|'||employee_pay_history__record_valid_from
    ) AS _pit_hook__employee_pay_history,
    *
  FROM validity
)
SELECT
  _pit_hook__employee_pay_history::BLOB,
  _hook__employee_pay_history::BLOB,
  _hook__person__employee::BLOB,
  _hook__epoch__rate_change_date::BLOB,
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
  employee_pay_history__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND employee_pay_history__record_updated_at BETWEEN @start_ts AND @end_ts