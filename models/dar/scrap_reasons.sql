MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__scrap_reason),
  description 'Business viewpoint of scrap_reasons data: Manufacturing failure reasons lookup table.',
  column_descriptions (
    scrap_reason__scrap_reason_id = 'Primary key for ScrapReason records.',
    scrap_reason__name = 'Failure description.',
    scrap_reason__modified_date = 'Date when this record was last modified',
    scrap_reason__record_loaded_at = 'Timestamp when this record was loaded into the system',
    scrap_reason__record_updated_at = 'Timestamp when this record was last updated',
    scrap_reason__record_version = 'Version number for this record',
    scrap_reason__record_valid_from = 'Timestamp from which this record version is valid',
    scrap_reason__record_valid_to = 'Timestamp until which this record version is valid',
    scrap_reason__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__scrap_reason,
    scrap_reason__scrap_reason_id,
    scrap_reason__name,
    scrap_reason__modified_date,
    scrap_reason__record_loaded_at,
    scrap_reason__record_updated_at,
    scrap_reason__record_version,
    scrap_reason__record_valid_from,
    scrap_reason__record_valid_to,
    scrap_reason__is_current_record
  FROM dab.bag__adventure_works__scrap_reasons
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__scrap_reason,
    NULL AS scrap_reason__scrap_reason_id,
    'N/A' AS scrap_reason__name,
    NULL AS scrap_reason__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS scrap_reason__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS scrap_reason__record_updated_at,
    0 AS scrap_reason__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS scrap_reason__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS scrap_reason__record_valid_to,
    TRUE AS scrap_reason__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__scrap_reason::BLOB,
  scrap_reason__scrap_reason_id::BIGINT,
  scrap_reason__name::TEXT,
  scrap_reason__modified_date::DATE,
  scrap_reason__record_loaded_at::TIMESTAMP,
  scrap_reason__record_updated_at::TIMESTAMP,
  scrap_reason__record_version::TEXT,
  scrap_reason__record_valid_from::TIMESTAMP,
  scrap_reason__record_valid_to::TIMESTAMP,
  scrap_reason__is_current_record::BOOLEAN
FROM cte__final