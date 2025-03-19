MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__address),
  description 'Business viewpoint of business_entity_addresses data: Cross-reference table mapping customers, vendors, and employees to their addresses.',
  column_descriptions (
    business_entity_address__business_entity_id = 'Primary key. Foreign key to BusinessEntity.BusinessEntityID.',
    business_entity_address__address_id = 'Primary key. Foreign key to Address.AddressID.',
    business_entity_address__address_type_id = 'Primary key. Foreign key to AddressType.AddressTypeID.',
    business_entity_address__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    business_entity_address__modified_date = 'Date when this record was last modified',
    business_entity_address__record_loaded_at = 'Timestamp when this record was loaded into the system',
    business_entity_address__record_updated_at = 'Timestamp when this record was last updated',
    business_entity_address__record_version = 'Version number for this record',
    business_entity_address__record_valid_from = 'Timestamp from which this record version is valid',
    business_entity_address__record_valid_to = 'Timestamp until which this record version is valid',
    business_entity_address__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__address,
    business_entity_address__business_entity_id,
    business_entity_address__address_id,
    business_entity_address__address_type_id,
    business_entity_address__rowguid,
    business_entity_address__modified_date,
    business_entity_address__record_loaded_at,
    business_entity_address__record_updated_at,
    business_entity_address__record_version,
    business_entity_address__record_valid_from,
    business_entity_address__record_valid_to,
    business_entity_address__is_current_record
  FROM dab.bag__adventure_works__business_entity_addresses
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__address,
    NULL AS business_entity_address__business_entity_id,
    NULL AS business_entity_address__address_id,
    NULL AS business_entity_address__address_type_id,
    'N/A' AS business_entity_address__rowguid,
    NULL AS business_entity_address__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS business_entity_address__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS business_entity_address__record_updated_at,
    0 AS business_entity_address__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS business_entity_address__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS business_entity_address__record_valid_to,
    TRUE AS business_entity_address__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__address::BLOB,
  business_entity_address__business_entity_id::BIGINT,
  business_entity_address__address_id::BIGINT,
  business_entity_address__address_type_id::BIGINT,
  business_entity_address__rowguid::TEXT,
  business_entity_address__modified_date::DATE,
  business_entity_address__record_loaded_at::TIMESTAMP,
  business_entity_address__record_updated_at::TIMESTAMP,
  business_entity_address__record_version::TEXT,
  business_entity_address__record_valid_from::TIMESTAMP,
  business_entity_address__record_valid_to::TIMESTAMP,
  business_entity_address__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.business_entity_addresses TO './export/dar/business_entity_addresses.parquet' (FORMAT parquet, COMPRESSION zstd)
);