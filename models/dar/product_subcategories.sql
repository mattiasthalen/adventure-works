MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_subcategory),
  description 'Business viewpoint of product_subcategories data: Product subcategories. See ProductCategory table.',
  column_descriptions (
    product_subcategory__product_subcategory_id = 'Primary key for ProductSubcategory records.',
    product_subcategory__product_category_id = 'Product category identification number. Foreign key to ProductCategory.ProductCategoryID.',
    product_subcategory__name = 'Subcategory description.',
    product_subcategory__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    product_subcategory__modified_date = 'Date when this record was last modified',
    product_subcategory__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_subcategory__record_updated_at = 'Timestamp when this record was last updated',
    product_subcategory__record_version = 'Version number for this record',
    product_subcategory__record_valid_from = 'Timestamp from which this record version is valid',
    product_subcategory__record_valid_to = 'Timestamp until which this record version is valid',
    product_subcategory__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__product_subcategory,
    product_subcategory__product_subcategory_id,
    product_subcategory__product_category_id,
    product_subcategory__name,
    product_subcategory__rowguid,
    product_subcategory__modified_date,
    product_subcategory__record_loaded_at,
    product_subcategory__record_updated_at,
    product_subcategory__record_version,
    product_subcategory__record_valid_from,
    product_subcategory__record_valid_to,
    product_subcategory__is_current_record
  FROM dab.bag__adventure_works__product_subcategories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__product_subcategory,
    NULL AS product_subcategory__product_subcategory_id,
    NULL AS product_subcategory__product_category_id,
    'N/A' AS product_subcategory__name,
    'N/A' AS product_subcategory__rowguid,
    NULL AS product_subcategory__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_subcategory__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_subcategory__record_updated_at,
    0 AS product_subcategory__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_subcategory__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_subcategory__record_valid_to,
    TRUE AS product_subcategory__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__product_subcategory::BLOB,
  product_subcategory__product_subcategory_id::BIGINT,
  product_subcategory__product_category_id::BIGINT,
  product_subcategory__name::TEXT,
  product_subcategory__rowguid::TEXT,
  product_subcategory__modified_date::DATE,
  product_subcategory__record_loaded_at::TIMESTAMP,
  product_subcategory__record_updated_at::TIMESTAMP,
  product_subcategory__record_version::TEXT,
  product_subcategory__record_valid_from::TIMESTAMP,
  product_subcategory__record_valid_to::TIMESTAMP,
  product_subcategory__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.product_subcategories TO './export/dar/product_subcategories.parquet' (FORMAT parquet, COMPRESSION zstd)
);