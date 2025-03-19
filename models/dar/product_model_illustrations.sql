MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_model_illustration),
  description 'Business viewpoint of product_model_illustrations data: Cross-reference table mapping product models and illustrations.',
  column_descriptions (
    product_model_illustration__product_model_id = 'Primary key. Foreign key to ProductModel.ProductModelID.',
    product_model_illustration__illustration_id = 'Primary key. Foreign key to Illustration.IllustrationID.',
    product_model_illustration__modified_date = 'Date when this record was last modified',
    product_model_illustration__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_model_illustration__record_updated_at = 'Timestamp when this record was last updated',
    product_model_illustration__record_version = 'Version number for this record',
    product_model_illustration__record_valid_from = 'Timestamp from which this record version is valid',
    product_model_illustration__record_valid_to = 'Timestamp until which this record version is valid',
    product_model_illustration__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__product_model_illustration,
    product_model_illustration__product_model_id,
    product_model_illustration__illustration_id,
    product_model_illustration__modified_date,
    product_model_illustration__record_loaded_at,
    product_model_illustration__record_updated_at,
    product_model_illustration__record_version,
    product_model_illustration__record_valid_from,
    product_model_illustration__record_valid_to,
    product_model_illustration__is_current_record
  FROM dab.bag__adventure_works__product_model_illustrations
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__product_model_illustration,
    NULL AS product_model_illustration__product_model_id,
    NULL AS product_model_illustration__illustration_id,
    NULL AS product_model_illustration__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_model_illustration__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_model_illustration__record_updated_at,
    0 AS product_model_illustration__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_model_illustration__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_model_illustration__record_valid_to,
    TRUE AS product_model_illustration__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__product_model_illustration::BLOB,
  product_model_illustration__product_model_id::BIGINT,
  product_model_illustration__illustration_id::BIGINT,
  product_model_illustration__modified_date::DATE,
  product_model_illustration__record_loaded_at::TIMESTAMP,
  product_model_illustration__record_updated_at::TIMESTAMP,
  product_model_illustration__record_version::TEXT,
  product_model_illustration__record_valid_from::TIMESTAMP,
  product_model_illustration__record_valid_to::TIMESTAMP,
  product_model_illustration__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.product_model_illustrations TO './export/dar/product_model_illustrations.parquet' (FORMAT parquet, COMPRESSION zstd)
);