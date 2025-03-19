MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_category),
  description 'Business viewpoint of product_categories data: High-level product categorization.',
  column_descriptions (
    product_category__product_category_id = 'Primary key for ProductCategory records.',
    product_category__name = 'Category description.',
    product_category__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    product_category__modified_date = 'Date when this record was last modified',
    product_category__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_category__record_updated_at = 'Timestamp when this record was last updated',
    product_category__record_version = 'Version number for this record',
    product_category__record_valid_from = 'Timestamp from which this record version is valid',
    product_category__record_valid_to = 'Timestamp until which this record version is valid',
    product_category__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__product_category,
    product_category__product_category_id,
    product_category__name,
    product_category__rowguid,
    product_category__modified_date,
    product_category__record_loaded_at,
    product_category__record_updated_at,
    product_category__record_version,
    product_category__record_valid_from,
    product_category__record_valid_to,
    product_category__is_current_record
  FROM dab.bag__adventure_works__product_categories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__product_category,
    NULL AS product_category__product_category_id,
    'N/A' AS product_category__name,
    'N/A' AS product_category__rowguid,
    NULL AS product_category__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_category__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_category__record_updated_at,
    0 AS product_category__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_category__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_category__record_valid_to,
    TRUE AS product_category__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__product_category::BLOB,
  product_category__product_category_id::BIGINT,
  product_category__name::TEXT,
  product_category__rowguid::TEXT,
  product_category__modified_date::DATE,
  product_category__record_loaded_at::TIMESTAMP,
  product_category__record_updated_at::TIMESTAMP,
  product_category__record_version::TEXT,
  product_category__record_valid_from::TIMESTAMP,
  product_category__record_valid_to::TIMESTAMP,
  product_category__is_current_record::BOOLEAN
FROM cte__final