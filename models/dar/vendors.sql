MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__vendor),
  description 'Business viewpoint of vendors data: Companies from whom Adventure Works Cycles purchases parts or other goods.',
  column_descriptions (
    vendor__business_entity_id = 'Primary key for Vendor records. Foreign key to BusinessEntity.BusinessEntityID.',
    vendor__account_number = 'Vendor account (identification) number.',
    vendor__name = 'Company name.',
    vendor__credit_rating = '1 = Superior, 2 = Excellent, 3 = Above average, 4 = Average, 5 = Below average.',
    vendor__preferred_vendor_status = '0 = Do not use if another vendor is available. 1 = Preferred over other vendors supplying the same product.',
    vendor__active_flag = '0 = Vendor no longer used. 1 = Vendor is actively used.',
    vendor__purchasing_web_service_url = 'Vendor URL.',
    vendor__modified_date = 'Date when this record was last modified',
    vendor__record_loaded_at = 'Timestamp when this record was loaded into the system',
    vendor__record_updated_at = 'Timestamp when this record was last updated',
    vendor__record_version = 'Version number for this record',
    vendor__record_valid_from = 'Timestamp from which this record version is valid',
    vendor__record_valid_to = 'Timestamp until which this record version is valid',
    vendor__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__vendor,
    vendor__business_entity_id,
    vendor__account_number,
    vendor__name,
    vendor__credit_rating,
    vendor__preferred_vendor_status,
    vendor__active_flag,
    vendor__purchasing_web_service_url,
    vendor__modified_date,
    vendor__record_loaded_at,
    vendor__record_updated_at,
    vendor__record_version,
    vendor__record_valid_from,
    vendor__record_valid_to,
    vendor__is_current_record
  FROM dab.bag__adventure_works__vendors
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__vendor,
    NULL AS vendor__business_entity_id,
    'N/A' AS vendor__account_number,
    'N/A' AS vendor__name,
    NULL AS vendor__credit_rating,
    FALSE AS vendor__preferred_vendor_status,
    FALSE AS vendor__active_flag,
    'N/A' AS vendor__purchasing_web_service_url,
    NULL AS vendor__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS vendor__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS vendor__record_updated_at,
    0 AS vendor__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS vendor__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS vendor__record_valid_to,
    TRUE AS vendor__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__vendor::BLOB,
  vendor__business_entity_id::BIGINT,
  vendor__account_number::TEXT,
  vendor__name::TEXT,
  vendor__credit_rating::BIGINT,
  vendor__preferred_vendor_status::BOOLEAN,
  vendor__active_flag::BOOLEAN,
  vendor__purchasing_web_service_url::TEXT,
  vendor__modified_date::DATE,
  vendor__record_loaded_at::TIMESTAMP,
  vendor__record_updated_at::TIMESTAMP,
  vendor__record_version::TEXT,
  vendor__record_valid_from::TIMESTAMP,
  vendor__record_valid_to::TIMESTAMP,
  vendor__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.vendors TO './export/dar/vendors.parquet' (FORMAT parquet, COMPRESSION zstd)
);