MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__sales_reason),
  description 'Business viewpoint of sales_reasons data: Lookup table of customer purchase reasons.',
  column_descriptions (
    sales_reason__sales_reason_id = 'Primary key for SalesReason records.',
    sales_reason__name = 'Sales reason description.',
    sales_reason__reason_type = 'Category the sales reason belongs to.',
    sales_reason__modified_date = 'Date when this record was last modified',
    sales_reason__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_reason__record_updated_at = 'Timestamp when this record was last updated',
    sales_reason__record_version = 'Version number for this record',
    sales_reason__record_valid_from = 'Timestamp from which this record version is valid',
    sales_reason__record_valid_to = 'Timestamp until which this record version is valid',
    sales_reason__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__sales_reason,
    sales_reason__sales_reason_id,
    sales_reason__name,
    sales_reason__reason_type,
    sales_reason__modified_date,
    sales_reason__record_loaded_at,
    sales_reason__record_updated_at,
    sales_reason__record_version,
    sales_reason__record_valid_from,
    sales_reason__record_valid_to,
    sales_reason__is_current_record
  FROM dab.bag__adventure_works__sales_reasons
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__sales_reason,
    NULL AS sales_reason__sales_reason_id,
    'N/A' AS sales_reason__name,
    'N/A' AS sales_reason__reason_type,
    NULL AS sales_reason__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_reason__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_reason__record_updated_at,
    0 AS sales_reason__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_reason__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS sales_reason__record_valid_to,
    TRUE AS sales_reason__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__sales_reason::BLOB,
  sales_reason__sales_reason_id::BIGINT,
  sales_reason__name::TEXT,
  sales_reason__reason_type::TEXT,
  sales_reason__modified_date::DATE,
  sales_reason__record_loaded_at::TIMESTAMP,
  sales_reason__record_updated_at::TIMESTAMP,
  sales_reason__record_version::TEXT,
  sales_reason__record_valid_from::TIMESTAMP,
  sales_reason__record_valid_to::TIMESTAMP,
  sales_reason__is_current_record::BOOLEAN
FROM cte__final