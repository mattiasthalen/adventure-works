MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__product_model),
  description 'Business viewpoint of product_models data: Product model classification.',
  column_descriptions (
    product_model__product_model_id = 'Primary key for ProductModel records.',
    product_model__name = 'Product model description.',
    product_model__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    product_model__catalog_description = 'Detailed product catalog information in xml format.',
    product_model__instructions = 'Manufacturing instructions in xml format.',
    product_model__modified_date = 'Date when this record was last modified',
    product_model__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_model__record_updated_at = 'Timestamp when this record was last updated',
    product_model__record_version = 'Version number for this record',
    product_model__record_valid_from = 'Timestamp from which this record version is valid',
    product_model__record_valid_to = 'Timestamp until which this record version is valid',
    product_model__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__product_model,
    product_model__product_model_id,
    product_model__name,
    product_model__rowguid,
    product_model__catalog_description,
    product_model__instructions,
    product_model__modified_date,
    product_model__record_loaded_at,
    product_model__record_updated_at,
    product_model__record_version,
    product_model__record_valid_from,
    product_model__record_valid_to,
    product_model__is_current_record
  FROM dab.bag__adventure_works__product_models
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__product_model,
    NULL AS product_model__product_model_id,
    'N/A' AS product_model__name,
    'N/A' AS product_model__rowguid,
    'N/A' AS product_model__catalog_description,
    'N/A' AS product_model__instructions,
    NULL AS product_model__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_model__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_model__record_updated_at,
    0 AS product_model__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_model__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_model__record_valid_to,
    TRUE AS product_model__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__product_model::BLOB,
  product_model__product_model_id::BIGINT,
  product_model__name::TEXT,
  product_model__rowguid::TEXT,
  product_model__catalog_description::TEXT,
  product_model__instructions::TEXT,
  product_model__modified_date::DATE,
  product_model__record_loaded_at::TIMESTAMP,
  product_model__record_updated_at::TIMESTAMP,
  product_model__record_version::TEXT,
  product_model__record_valid_from::TIMESTAMP,
  product_model__record_valid_to::TIMESTAMP,
  product_model__is_current_record::BOOLEAN
FROM cte__final