MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__shift
  ),
  tags hook,
  grain (_pit_hook__reference__shift, _hook__reference__shift),
  description 'Hook viewpoint of shifts data: Work shift lookup table.',
  column_descriptions (
    shift__shift_id = 'Primary key for Shift records.',
    shift__name = 'Shift description.',
    shift__start_time = 'Shift start time. ISO duration.',
    shift__end_time = 'Shift end time. ISO duration.',
    shift__record_loaded_at = 'Timestamp when this record was loaded into the system',
    shift__record_updated_at = 'Timestamp when this record was last updated',
    shift__record_version = 'Version number for this record',
    shift__record_valid_from = 'Timestamp from which this record version is valid',
    shift__record_valid_to = 'Timestamp until which this record version is valid',
    shift__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__shift = 'Reference hook to shift reference',
    _pit_hook__reference__shift = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    shift_id AS shift__shift_id,
    name AS shift__name,
    MAKE_TIME(REGEXP_EXTRACT(start_time, 'PT(\d+)H', 1)::INT, 0, 0) AS shift__start_time,
    MAKE_TIME(REGEXP_EXTRACT(end_time, 'PT(\d+)H', 1)::INT, 0, 0) AS shift__end_time,
    modified_date AS shift__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS shift__record_loaded_at
  FROM das.raw__adventure_works__shifts
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY shift__shift_id ORDER BY shift__record_loaded_at) AS shift__record_version,
    CASE
      WHEN shift__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE shift__record_loaded_at
    END AS shift__record_valid_from,
    COALESCE(
      LEAD(shift__record_loaded_at) OVER (PARTITION BY shift__shift_id ORDER BY shift__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS shift__record_valid_to,
    shift__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS shift__is_current_record,
    CASE
      WHEN shift__is_current_record
      THEN shift__record_loaded_at
      ELSE shift__record_valid_to
    END AS shift__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__shift__adventure_works|', shift__shift_id) AS _hook__reference__shift,
    CONCAT_WS('~',
      _hook__reference__shift,
      'epoch__valid_from|'||shift__record_valid_from
    ) AS _pit_hook__reference__shift,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__shift::BLOB,
  _hook__reference__shift::BLOB,
  shift__shift_id::BIGINT,
  shift__name::TEXT,
  shift__start_time::TIME,
  shift__end_time::TIME,
  shift__modified_date::DATE,
  shift__record_loaded_at::TIMESTAMP,
  shift__record_updated_at::TIMESTAMP,
  shift__record_version::TEXT,
  shift__record_valid_from::TIMESTAMP,
  shift__record_valid_to::TIMESTAMP,
  shift__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND shift__record_updated_at BETWEEN @start_ts AND @end_ts