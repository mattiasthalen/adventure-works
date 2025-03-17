MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__unit_measure
  ),
  tags hook,
  grain (_pit_hook__reference__unit_measure, _hook__reference__unit_measure),
  description 'Hook viewpoint of unit_measures data: Unit of measure lookup table.',
  column_descriptions (
    unit_measure__unit_measure_code = 'Primary key.',
    unit_measure__name = 'Unit of measure description.',
    unit_measure__record_loaded_at = 'Timestamp when this record was loaded into the system',
    unit_measure__record_updated_at = 'Timestamp when this record was last updated',
    unit_measure__record_version = 'Version number for this record',
    unit_measure__record_valid_from = 'Timestamp from which this record version is valid',
    unit_measure__record_valid_to = 'Timestamp until which this record version is valid',
    unit_measure__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__unit_measure = 'Reference hook to unit_measure reference',
    _pit_hook__reference__unit_measure = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    unit_measure_code AS unit_measure__unit_measure_code,
    name AS unit_measure__name,
    modified_date AS unit_measure__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS unit_measure__record_loaded_at
  FROM das.raw__adventure_works__unit_measures
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY unit_measure__unit_measure_code ORDER BY unit_measure__record_loaded_at) AS unit_measure__record_version,
    CASE
      WHEN unit_measure__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE unit_measure__record_loaded_at
    END AS unit_measure__record_valid_from,
    COALESCE(
      LEAD(unit_measure__record_loaded_at) OVER (PARTITION BY unit_measure__unit_measure_code ORDER BY unit_measure__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS unit_measure__record_valid_to,
    unit_measure__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS unit_measure__is_current_record,
    CASE
      WHEN unit_measure__is_current_record
      THEN unit_measure__record_loaded_at
      ELSE unit_measure__record_valid_to
    END AS unit_measure__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__unit_measure__adventure_works|', unit_measure__unit_measure_code) AS _hook__reference__unit_measure,
    CONCAT_WS('~',
      _hook__reference__unit_measure,
      'epoch__valid_from|'||unit_measure__record_valid_from
    ) AS _pit_hook__reference__unit_measure,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__unit_measure::BLOB,
  _hook__reference__unit_measure::BLOB,
  unit_measure__unit_measure_code::TEXT,
  unit_measure__name::TEXT,
  unit_measure__modified_date::DATE,
  unit_measure__record_loaded_at::TIMESTAMP,
  unit_measure__record_updated_at::TIMESTAMP,
  unit_measure__record_version::TEXT,
  unit_measure__record_valid_from::TIMESTAMP,
  unit_measure__record_valid_to::TIMESTAMP,
  unit_measure__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND unit_measure__record_updated_at BETWEEN @start_ts AND @end_ts