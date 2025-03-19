MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__individual),
  description 'Business viewpoint of email_addresses data: Where to send a person email.',
  column_descriptions (
    email_address__business_entity_id = 'Primary key. Person associated with this email address. Foreign key to Person.BusinessEntityID.',
    email_address__email_address_id = 'Primary key. ID of this email address.',
    email_address__email = 'E-mail address for the person.',
    email_address__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    email_address__modified_date = 'Date when this record was last modified',
    email_address__record_loaded_at = 'Timestamp when this record was loaded into the system',
    email_address__record_updated_at = 'Timestamp when this record was last updated',
    email_address__record_version = 'Version number for this record',
    email_address__record_valid_from = 'Timestamp from which this record version is valid',
    email_address__record_valid_to = 'Timestamp until which this record version is valid',
    email_address__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__person__individual,
    email_address__business_entity_id,
    email_address__email_address_id,
    email_address__email,
    email_address__rowguid,
    email_address__modified_date,
    email_address__record_loaded_at,
    email_address__record_updated_at,
    email_address__record_version,
    email_address__record_valid_from,
    email_address__record_valid_to,
    email_address__is_current_record
  FROM dab.bag__adventure_works__email_addresses
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__person__individual,
    NULL AS email_address__business_entity_id,
    NULL AS email_address__email_address_id,
    'N/A' AS email_address__email,
    'N/A' AS email_address__rowguid,
    NULL AS email_address__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS email_address__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS email_address__record_updated_at,
    0 AS email_address__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS email_address__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS email_address__record_valid_to,
    TRUE AS email_address__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__person__individual::BLOB,
  email_address__business_entity_id::BIGINT,
  email_address__email_address_id::BIGINT,
  email_address__email::TEXT,
  email_address__rowguid::TEXT,
  email_address__modified_date::DATE,
  email_address__record_loaded_at::TIMESTAMP,
  email_address__record_updated_at::TIMESTAMP,
  email_address__record_version::TEXT,
  email_address__record_valid_from::TIMESTAMP,
  email_address__record_valid_to::TIMESTAMP,
  email_address__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.email_addresses TO './export/dar/email_addresses.parquet' (FORMAT parquet, COMPRESSION zstd)
);