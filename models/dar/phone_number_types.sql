MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__phone_number_type),
  description 'Business viewpoint of phone_number_types data: Type of phone number of a person.',
  column_descriptions (
    phone_number_type__phone_number_type_id = 'Primary key for telephone number type records.',
    phone_number_type__name = 'Name of the telephone number type.',
    phone_number_type__modified_date = 'Date when this record was last modified',
    phone_number_type__record_loaded_at = 'Timestamp when this record was loaded into the system',
    phone_number_type__record_updated_at = 'Timestamp when this record was last updated',
    phone_number_type__record_version = 'Version number for this record',
    phone_number_type__record_valid_from = 'Timestamp from which this record version is valid',
    phone_number_type__record_valid_to = 'Timestamp until which this record version is valid',
    phone_number_type__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__phone_number_type,
    phone_number_type__phone_number_type_id,
    phone_number_type__name,
    phone_number_type__modified_date,
    phone_number_type__record_loaded_at,
    phone_number_type__record_updated_at,
    phone_number_type__record_version,
    phone_number_type__record_valid_from,
    phone_number_type__record_valid_to,
    phone_number_type__is_current_record
  FROM dab.bag__adventure_works__phone_number_types
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__phone_number_type,
    NULL AS phone_number_type__phone_number_type_id,
    'N/A' AS phone_number_type__name,
    NULL AS phone_number_type__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS phone_number_type__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS phone_number_type__record_updated_at,
    0 AS phone_number_type__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS phone_number_type__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS phone_number_type__record_valid_to,
    TRUE AS phone_number_type__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__phone_number_type::BLOB,
  phone_number_type__phone_number_type_id::BIGINT,
  phone_number_type__name::TEXT,
  phone_number_type__modified_date::DATE,
  phone_number_type__record_loaded_at::TIMESTAMP,
  phone_number_type__record_updated_at::TIMESTAMP,
  phone_number_type__record_version::TEXT,
  phone_number_type__record_valid_from::TIMESTAMP,
  phone_number_type__record_valid_to::TIMESTAMP,
  phone_number_type__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.phone_number_types TO './export/dar/phone_number_types.parquet' (FORMAT parquet, COMPRESSION zstd)
);