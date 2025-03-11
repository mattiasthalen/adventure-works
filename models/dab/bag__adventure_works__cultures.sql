MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__culture
  ),
  tags hook,
  grain (_pit_hook__reference__culture, _hook__reference__culture)
);

WITH staging AS (
  SELECT
    culture_id AS culture__culture_id,
    name AS culture__name,
    modified_date AS culture__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS culture__record_loaded_at
  FROM das.raw__adventure_works__cultures
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY culture__culture_id ORDER BY culture__record_loaded_at) AS culture__record_version,
    CASE
      WHEN culture__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE culture__record_loaded_at
    END AS culture__record_valid_from,
    COALESCE(
      LEAD(culture__record_loaded_at) OVER (PARTITION BY culture__culture_id ORDER BY culture__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS culture__record_valid_to,
    culture__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS culture__is_current_record,
    CASE
      WHEN culture__is_current_record
      THEN culture__record_loaded_at
      ELSE culture__record_valid_to
    END AS culture__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__culture__adventure_works|', culture__culture_id) AS _hook__reference__culture,
    CONCAT_WS('~',
      _hook__reference__culture,
      'epoch__valid_from|'||culture__record_valid_from
    ) AS _pit_hook__reference__culture,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__culture::BLOB,
  _hook__reference__culture::BLOB,
  culture__culture_id::TEXT,
  culture__name::TEXT,
  culture__modified_date::DATE,
  culture__record_loaded_at::TIMESTAMP,
  culture__record_updated_at::TIMESTAMP,
  culture__record_version::TEXT,
  culture__record_valid_from::TIMESTAMP,
  culture__record_valid_to::TIMESTAMP,
  culture__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND culture__record_updated_at BETWEEN @start_ts AND @end_ts