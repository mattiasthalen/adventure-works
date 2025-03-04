MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column employee_department_histori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS employee_department_histori__business_entity_id,
    department_id AS employee_department_histori__department_id,
    shift_id AS employee_department_histori__shift_id,
    end_date AS employee_department_histori__end_date,
    modified_date AS employee_department_histori__modified_date,
    start_date AS employee_department_histori__start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS employee_department_histori__record_loaded_at
  FROM bronze.raw__adventure_works__employee_department_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY employee_department_histori__business_entity_id ORDER BY employee_department_histori__record_loaded_at) AS employee_department_histori__record_version,
    CASE
      WHEN employee_department_histori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE employee_department_histori__record_loaded_at
    END AS employee_department_histori__record_valid_from,
    COALESCE(
      LEAD(employee_department_histori__record_loaded_at) OVER (PARTITION BY employee_department_histori__business_entity_id ORDER BY employee_department_histori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS employee_department_histori__record_valid_to,
    employee_department_histori__record_valid_to = @max_ts::TIMESTAMP AS employee_department_histori__is_current_record,
    CASE
      WHEN employee_department_histori__is_current_record
      THEN employee_department_histori__record_loaded_at
      ELSE employee_department_histori__record_valid_to
    END AS employee_department_histori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      employee_department_histori__business_entity_id,
      '~epoch|valid_from|',
      employee_department_histori__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', employee_department_histori__business_entity_id) AS _hook__business_entity,
    CONCAT('department|adventure_works|', employee_department_histori__department_id) AS _hook__department,
    CONCAT('shift|adventure_works|', employee_department_histori__shift_id) AS _hook__shift,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__department::BLOB,
  _hook__shift::BLOB,
  employee_department_histori__business_entity_id::BIGINT,
  employee_department_histori__department_id::BIGINT,
  employee_department_histori__shift_id::BIGINT,
  employee_department_histori__end_date::VARCHAR,
  employee_department_histori__modified_date::VARCHAR,
  employee_department_histori__start_date::TIMESTAMP,
  employee_department_histori__record_loaded_at::TIMESTAMP,
  employee_department_histori__record_updated_at::TIMESTAMP,
  employee_department_histori__record_valid_from::TIMESTAMP,
  employee_department_histori__record_valid_to::TIMESTAMP,
  employee_department_histori__record_version::INT,
  employee_department_histori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND employee_department_histori__record_updated_at BETWEEN @start_ts AND @end_ts