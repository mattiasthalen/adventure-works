MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__illustration),
  description 'Business viewpoint of illustrations data: Bicycle assembly diagrams.',
  column_descriptions (
    illustration__illustration_id = 'Primary key for Illustration records.',
    illustration__diagram = 'Illustrations used in manufacturing instructions. Stored as XML.',
    illustration__modified_date = 'Date when this record was last modified',
    illustration__record_loaded_at = 'Timestamp when this record was loaded into the system',
    illustration__record_updated_at = 'Timestamp when this record was last updated',
    illustration__record_version = 'Version number for this record',
    illustration__record_valid_from = 'Timestamp from which this record version is valid',
    illustration__record_valid_to = 'Timestamp until which this record version is valid',
    illustration__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__illustration,
    illustration__illustration_id,
    illustration__diagram,
    illustration__modified_date,
    illustration__record_loaded_at,
    illustration__record_updated_at,
    illustration__record_version,
    illustration__record_valid_from,
    illustration__record_valid_to,
    illustration__is_current_record
  FROM dab.bag__adventure_works__illustrations
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__illustration,
    NULL AS illustration__illustration_id,
    'N/A' AS illustration__diagram,
    NULL AS illustration__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS illustration__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS illustration__record_updated_at,
    0 AS illustration__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS illustration__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS illustration__record_valid_to,
    TRUE AS illustration__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__illustration::BLOB,
  illustration__illustration_id::BIGINT,
  illustration__diagram::TEXT,
  illustration__modified_date::DATE,
  illustration__record_loaded_at::TIMESTAMP,
  illustration__record_updated_at::TIMESTAMP,
  illustration__record_version::TEXT,
  illustration__record_valid_from::TIMESTAMP,
  illustration__record_valid_to::TIMESTAMP,
  illustration__is_current_record::BOOLEAN
FROM cte__final