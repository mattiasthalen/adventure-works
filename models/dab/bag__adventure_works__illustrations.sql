MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__illustration
  ),
  tags hook,
  grain (_pit_hook__reference__illustration, _hook__reference__illustration)
);

WITH staging AS (
  SELECT
    illustration_id AS illustration__illustration_id,
    diagram AS illustration__diagram,
    modified_date AS illustration__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS illustration__record_loaded_at
  FROM das.raw__adventure_works__illustrations
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY illustration__illustration_id ORDER BY illustration__record_loaded_at) AS illustration__record_version,
    CASE
      WHEN illustration__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE illustration__record_loaded_at
    END AS illustration__record_valid_from,
    COALESCE(
      LEAD(illustration__record_loaded_at) OVER (PARTITION BY illustration__illustration_id ORDER BY illustration__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS illustration__record_valid_to,
    illustration__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS illustration__is_current_record,
    CASE
      WHEN illustration__is_current_record
      THEN illustration__record_loaded_at
      ELSE illustration__record_valid_to
    END AS illustration__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__illustration__adventure_works|', illustration__illustration_id) AS _hook__reference__illustration,
    CONCAT_WS('~',
      _hook__reference__illustration,
      'epoch__valid_from|'||illustration__record_valid_from
    ) AS _pit_hook__reference__illustration,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__illustration::BLOB,
  _hook__reference__illustration::BLOB,
  illustration__illustration_id::BIGINT,
  illustration__diagram::TEXT,
  illustration__modified_date::DATE,
  illustration__record_loaded_at::TIMESTAMP,
  illustration__record_updated_at::TIMESTAMP,
  illustration__record_version::TEXT,
  illustration__record_valid_from::TIMESTAMP,
  illustration__record_valid_to::TIMESTAMP,
  illustration__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND illustration__record_updated_at BETWEEN @start_ts AND @end_ts