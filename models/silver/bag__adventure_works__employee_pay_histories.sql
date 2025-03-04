MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column employee_pay_histori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS employee_pay_histori__business_entity_id,
    rate_change_date AS employee_pay_histori__rate_change_date,
    modified_date AS employee_pay_histori__modified_date,
    pay_frequency AS employee_pay_histori__pay_frequency,
    rate AS employee_pay_histori__rate,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS employee_pay_histori__record_loaded_at
  FROM bronze.raw__adventure_works__employee_pay_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY employee_pay_histori__business_entity_id ORDER BY employee_pay_histori__record_loaded_at) AS employee_pay_histori__record_version,
    CASE
      WHEN employee_pay_histori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE employee_pay_histori__record_loaded_at
    END AS employee_pay_histori__record_valid_from,
    COALESCE(
      LEAD(employee_pay_histori__record_loaded_at) OVER (PARTITION BY employee_pay_histori__business_entity_id ORDER BY employee_pay_histori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS employee_pay_histori__record_valid_to,
    employee_pay_histori__record_valid_to = @max_ts::TIMESTAMP AS employee_pay_histori__is_current_record,
    CASE
      WHEN employee_pay_histori__is_current_record
      THEN employee_pay_histori__record_loaded_at
      ELSE employee_pay_histori__record_valid_to
    END AS employee_pay_histori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      employee_pay_histori__business_entity_id,
      '~epoch|valid_from|',
      employee_pay_histori__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', employee_pay_histori__business_entity_id) AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  employee_pay_histori__business_entity_id::BIGINT,
  employee_pay_histori__modified_date::VARCHAR,
  employee_pay_histori__pay_frequency::BIGINT,
  employee_pay_histori__rate::DOUBLE,
  employee_pay_histori__record_loaded_at::TIMESTAMP,
  employee_pay_histori__record_updated_at::TIMESTAMP,
  employee_pay_histori__record_valid_from::TIMESTAMP,
  employee_pay_histori__record_valid_to::TIMESTAMP,
  employee_pay_histori__record_version::INT,
  employee_pay_histori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND employee_pay_histori__record_updated_at BETWEEN @start_ts AND @end_ts