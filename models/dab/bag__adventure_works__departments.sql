MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__department
  ),
  tags hook,
  grain (_pit_hook__department, _hook__department),
  description 'Hook viewpoint of departments data: Lookup table containing the departments within the Adventure Works Cycles company.',
  column_descriptions (
    department__department_id = 'Primary key for Department records.',
    department__name = 'Name of the department.',
    department__group_name = 'Name of the group to which the department belongs.',
    department__record_loaded_at = 'Timestamp when this record was loaded into the system',
    department__record_updated_at = 'Timestamp when this record was last updated',
    department__record_version = 'Version number for this record',
    department__record_valid_from = 'Timestamp from which this record version is valid',
    department__record_valid_to = 'Timestamp until which this record version is valid',
    department__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__department = 'Reference hook to department',
    _pit_hook__department = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    department_id AS department__department_id,
    name AS department__name,
    group_name AS department__group_name,
    modified_date AS department__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS department__record_loaded_at
  FROM das.raw__adventure_works__departments
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY department__department_id ORDER BY department__record_loaded_at) AS department__record_version,
    CASE
      WHEN department__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE department__record_loaded_at
    END AS department__record_valid_from,
    COALESCE(
      LEAD(department__record_loaded_at) OVER (PARTITION BY department__department_id ORDER BY department__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS department__record_valid_to,
    department__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS department__is_current_record,
    CASE
      WHEN department__is_current_record
      THEN department__record_loaded_at
      ELSE department__record_valid_to
    END AS department__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('department__adventure_works|', department__department_id) AS _hook__department,
    CONCAT_WS('~',
      _hook__department,
      'epoch__valid_from|'||department__record_valid_from
    ) AS _pit_hook__department,
    *
  FROM validity
)
SELECT
  _pit_hook__department::BLOB,
  _hook__department::BLOB,
  department__department_id::BIGINT,
  department__name::TEXT,
  department__group_name::TEXT,
  department__modified_date::DATE,
  department__record_loaded_at::TIMESTAMP,
  department__record_updated_at::TIMESTAMP,
  department__record_version::TEXT,
  department__record_valid_from::TIMESTAMP,
  department__record_valid_to::TIMESTAMP,
  department__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND department__record_updated_at BETWEEN @start_ts AND @end_ts