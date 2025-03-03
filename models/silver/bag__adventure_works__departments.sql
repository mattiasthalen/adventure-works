MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    department_id AS department__department_id,
    group_name AS department__group_name,
    modified_date AS department__modified_date,
    name AS department__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS department__record_loaded_at
  FROM bronze.raw__adventure_works__departments
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY department__department_id ORDER BY department__record_loaded_at) AS department__record_version,
    CASE
      WHEN department__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE department__record_loaded_at
    END AS department__record_valid_from,
    COALESCE(
      LEAD(department__record_loaded_at) OVER (PARTITION BY department__department_id ORDER BY department__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS department__record_valid_to,
    department__record_valid_to = @max_ts::TIMESTAMP AS department__is_current_record,
    CASE
      WHEN department__is_current_record
      THEN department__record_loaded_at
      ELSE department__record_valid_to
    END AS department__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'department|adventure_works|',
      department__department_id,
      '~epoch|valid_from|',
      department__record_valid_from
    ) AS _pit_hook__department,
    CONCAT('department|adventure_works|', department__department_id) AS _hook__department,
    *
  FROM validity
)
SELECT
  _pit_hook__department::BLOB,
  _hook__department::BLOB,
  department__department_id::VARCHAR,
  department__group_name::VARCHAR,
  department__modified_date::VARCHAR,
  department__name::VARCHAR,
  department__record_loaded_at::TIMESTAMP,
  department__record_version::INT,
  department__record_valid_from::TIMESTAMP,
  department__record_valid_to::TIMESTAMP,
  department__is_current_record::BOOLEAN,
  department__record_updated_at::TIMESTAMP
FROM hooks