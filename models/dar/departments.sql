MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__department),
  description 'Business viewpoint of departments data: Lookup table containing the departments within the Adventure Works Cycles company.',
  column_descriptions (
    department__department_id = 'Primary key for Department records.',
    department__name = 'Name of the department.',
    department__group_name = 'Name of the group to which the department belongs.',
    department__modified_date = 'Date when this record was last modified',
    department__record_loaded_at = 'Timestamp when this record was loaded into the system',
    department__record_updated_at = 'Timestamp when this record was last updated',
    department__record_version = 'Version number for this record',
    department__record_valid_from = 'Timestamp from which this record version is valid',
    department__record_valid_to = 'Timestamp until which this record version is valid',
    department__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__department,
    department__department_id,
    department__name,
    department__group_name,
    department__modified_date,
    department__record_loaded_at,
    department__record_updated_at,
    department__record_version,
    department__record_valid_from,
    department__record_valid_to,
    department__is_current_record
  FROM dab.bag__adventure_works__departments
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__department,
    NULL AS department__department_id,
    'N/A' AS department__name,
    'N/A' AS department__group_name,
    NULL AS department__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS department__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS department__record_updated_at,
    0 AS department__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS department__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS department__record_valid_to,
    TRUE AS department__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__department::BLOB,
  department__department_id::BIGINT,
  department__name::TEXT,
  department__group_name::TEXT,
  department__modified_date::DATE,
  department__record_loaded_at::TIMESTAMP,
  department__record_updated_at::TIMESTAMP,
  department__record_version::TEXT,
  department__record_valid_from::TIMESTAMP,
  department__record_valid_to::TIMESTAMP,
  department__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.departments TO './export/dar/departments.parquet' (FORMAT parquet, COMPRESSION zstd)
);