MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__scrap_reason
  ),
  tags hook,
  grain (_pit_hook__reference__scrap_reason, _hook__reference__scrap_reason),
  description 'Hook viewpoint of scrap_reasons data: Manufacturing failure reasons lookup table.',
  column_descriptions (
    scrap_reason__scrap_reason_id = 'Primary key for ScrapReason records.',
    scrap_reason__name = 'Failure description.',
    scrap_reason__record_loaded_at = 'Timestamp when this record was loaded into the system',
    scrap_reason__record_updated_at = 'Timestamp when this record was last updated',
    scrap_reason__record_version = 'Version number for this record',
    scrap_reason__record_valid_from = 'Timestamp from which this record version is valid',
    scrap_reason__record_valid_to = 'Timestamp until which this record version is valid',
    scrap_reason__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__scrap_reason = 'Reference hook to scrap_reason reference',
    _pit_hook__reference__scrap_reason = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    scrap_reason_id AS scrap_reason__scrap_reason_id,
    name AS scrap_reason__name,
    modified_date AS scrap_reason__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS scrap_reason__record_loaded_at
  FROM das.raw__adventure_works__scrap_reasons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY scrap_reason__scrap_reason_id ORDER BY scrap_reason__record_loaded_at) AS scrap_reason__record_version,
    CASE
      WHEN scrap_reason__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE scrap_reason__record_loaded_at
    END AS scrap_reason__record_valid_from,
    COALESCE(
      LEAD(scrap_reason__record_loaded_at) OVER (PARTITION BY scrap_reason__scrap_reason_id ORDER BY scrap_reason__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS scrap_reason__record_valid_to,
    scrap_reason__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS scrap_reason__is_current_record,
    CASE
      WHEN scrap_reason__is_current_record
      THEN scrap_reason__record_loaded_at
      ELSE scrap_reason__record_valid_to
    END AS scrap_reason__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__scrap_reason__adventure_works|', scrap_reason__scrap_reason_id) AS _hook__reference__scrap_reason,
    CONCAT_WS('~',
      _hook__reference__scrap_reason,
      'epoch__valid_from|'||scrap_reason__record_valid_from
    ) AS _pit_hook__reference__scrap_reason,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__scrap_reason::BLOB,
  _hook__reference__scrap_reason::BLOB,
  scrap_reason__scrap_reason_id::BIGINT,
  scrap_reason__name::TEXT,
  scrap_reason__modified_date::DATE,
  scrap_reason__record_loaded_at::TIMESTAMP,
  scrap_reason__record_updated_at::TIMESTAMP,
  scrap_reason__record_version::TEXT,
  scrap_reason__record_valid_from::TIMESTAMP,
  scrap_reason__record_valid_to::TIMESTAMP,
  scrap_reason__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND scrap_reason__record_updated_at BETWEEN @start_ts AND @end_ts