MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__product_description),
  description 'Business viewpoint of product_descriptions data: Product descriptions in several languages.',
  column_descriptions (
    product_description__product_description_id = 'Primary key for ProductDescription records.',
    product_description__description = 'Description of the product.',
    product_description__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    product_description__modified_date = 'Date when this record was last modified',
    product_description__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_description__record_updated_at = 'Timestamp when this record was last updated',
    product_description__record_version = 'Version number for this record',
    product_description__record_valid_from = 'Timestamp from which this record version is valid',
    product_description__record_valid_to = 'Timestamp until which this record version is valid',
    product_description__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__product_description,
    product_description__product_description_id,
    product_description__description,
    product_description__rowguid,
    product_description__modified_date,
    product_description__record_loaded_at,
    product_description__record_updated_at,
    product_description__record_version,
    product_description__record_valid_from,
    product_description__record_valid_to,
    product_description__is_current_record
  FROM dab.bag__adventure_works__product_descriptions
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__product_description,
    NULL AS product_description__product_description_id,
    'N/A' AS product_description__description,
    'N/A' AS product_description__rowguid,
    NULL AS product_description__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_description__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_description__record_updated_at,
    0 AS product_description__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_description__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_description__record_valid_to,
    TRUE AS product_description__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__product_description::BLOB,
  product_description__product_description_id::BIGINT,
  product_description__description::TEXT,
  product_description__rowguid::TEXT,
  product_description__modified_date::DATE,
  product_description__record_loaded_at::TIMESTAMP,
  product_description__record_updated_at::TIMESTAMP,
  product_description__record_version::TEXT,
  product_description__record_valid_from::TIMESTAMP,
  product_description__record_valid_to::TIMESTAMP,
  product_description__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.product_descriptions TO './export/dar/product_descriptions.parquet' (FORMAT parquet, COMPRESSION zstd)
);