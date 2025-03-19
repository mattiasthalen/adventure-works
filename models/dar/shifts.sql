MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__shift),
  description 'Business viewpoint of shifts data: Work shift lookup table.',
  column_descriptions (
    shift__shift_id = 'Primary key for Shift records.',
    shift__name = 'Shift description.',
    shift__start_time = 'Shift start time. ISO duration.',
    shift__end_time = 'Shift end time. ISO duration.',
    shift__modified_date = 'Date when this record was last modified',
    shift__record_loaded_at = 'Timestamp when this record was loaded into the system',
    shift__record_updated_at = 'Timestamp when this record was last updated',
    shift__record_version = 'Version number for this record',
    shift__record_valid_from = 'Timestamp from which this record version is valid',
    shift__record_valid_to = 'Timestamp until which this record version is valid',
    shift__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__shift,
    shift__shift_id,
    shift__name,
    shift__start_time,
    shift__end_time,
    shift__modified_date,
    shift__record_loaded_at,
    shift__record_updated_at,
    shift__record_version,
    shift__record_valid_from,
    shift__record_valid_to,
    shift__is_current_record
  FROM dab.bag__adventure_works__shifts
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__shift,
    NULL AS shift__shift_id,
    'N/A' AS shift__name,
    'N/A' AS shift__start_time,
    'N/A' AS shift__end_time,
    NULL AS shift__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS shift__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS shift__record_updated_at,
    0 AS shift__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS shift__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS shift__record_valid_to,
    TRUE AS shift__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__shift::BLOB,
  shift__shift_id::BIGINT,
  shift__name::TEXT,
  shift__start_time::TEXT,
  shift__end_time::TEXT,
  shift__modified_date::DATE,
  shift__record_loaded_at::TIMESTAMP,
  shift__record_updated_at::TIMESTAMP,
  shift__record_version::TEXT,
  shift__record_valid_from::TIMESTAMP,
  shift__record_valid_to::TIMESTAMP,
  shift__is_current_record::BOOLEAN
FROM cte__final