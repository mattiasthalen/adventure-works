MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__contact_type),
  description 'Business viewpoint of contact_types data: Lookup table containing the types of business entity contacts.',
  column_descriptions (
    contact_type__contact_type_id = 'Primary key for ContactType records.',
    contact_type__name = 'Contact type description.',
    contact_type__modified_date = 'Date when this record was last modified',
    contact_type__record_loaded_at = 'Timestamp when this record was loaded into the system',
    contact_type__record_updated_at = 'Timestamp when this record was last updated',
    contact_type__record_version = 'Version number for this record',
    contact_type__record_valid_from = 'Timestamp from which this record version is valid',
    contact_type__record_valid_to = 'Timestamp until which this record version is valid',
    contact_type__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__contact_type,
    contact_type__contact_type_id,
    contact_type__name,
    contact_type__modified_date,
    contact_type__record_loaded_at,
    contact_type__record_updated_at,
    contact_type__record_version,
    contact_type__record_valid_from,
    contact_type__record_valid_to,
    contact_type__is_current_record
  FROM dab.bag__adventure_works__contact_types
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__contact_type,
    NULL AS contact_type__contact_type_id,
    'N/A' AS contact_type__name,
    NULL AS contact_type__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS contact_type__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS contact_type__record_updated_at,
    0 AS contact_type__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS contact_type__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS contact_type__record_valid_to,
    TRUE AS contact_type__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__contact_type::BLOB,
  contact_type__contact_type_id::BIGINT,
  contact_type__name::TEXT,
  contact_type__modified_date::DATE,
  contact_type__record_loaded_at::TIMESTAMP,
  contact_type__record_updated_at::TIMESTAMP,
  contact_type__record_version::TEXT,
  contact_type__record_valid_from::TIMESTAMP,
  contact_type__record_valid_to::TIMESTAMP,
  contact_type__is_current_record::BOOLEAN
FROM cte__final