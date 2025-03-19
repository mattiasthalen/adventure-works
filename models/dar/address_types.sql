MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__address_type),
  description 'Business viewpoint of address_types data: Types of addresses stored in the Address table.',
  column_descriptions (
    address_type__address_type_id = 'Primary key for AddressType records.',
    address_type__name = 'Address type description. For example, Billing, Home, or Shipping.',
    address_type__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    address_type__modified_date = 'Date when this record was last modified',
    address_type__record_loaded_at = 'Timestamp when this record was loaded into the system',
    address_type__record_updated_at = 'Timestamp when this record was last updated',
    address_type__record_version = 'Version number for this record',
    address_type__record_valid_from = 'Timestamp from which this record version is valid',
    address_type__record_valid_to = 'Timestamp until which this record version is valid',
    address_type__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__address_type,
    address_type__address_type_id,
    address_type__name,
    address_type__rowguid,
    address_type__modified_date,
    address_type__record_loaded_at,
    address_type__record_updated_at,
    address_type__record_version,
    address_type__record_valid_from,
    address_type__record_valid_to,
    address_type__is_current_record
  FROM dab.bag__adventure_works__address_types
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__address_type,
    NULL AS address_type__address_type_id,
    'N/A' AS address_type__name,
    'N/A' AS address_type__rowguid,
    NULL AS address_type__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS address_type__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS address_type__record_updated_at,
    0 AS address_type__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS address_type__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS address_type__record_valid_to,
    TRUE AS address_type__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__address_type::BLOB,
  address_type__address_type_id::BIGINT,
  address_type__name::TEXT,
  address_type__rowguid::TEXT,
  address_type__modified_date::DATE,
  address_type__record_loaded_at::TIMESTAMP,
  address_type__record_updated_at::TIMESTAMP,
  address_type__record_version::TEXT,
  address_type__record_valid_from::TIMESTAMP,
  address_type__record_valid_to::TIMESTAMP,
  address_type__is_current_record::BOOLEAN
FROM cte__final