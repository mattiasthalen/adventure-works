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

SELECT
  *
  EXCLUDE (_hook__product_model_illustration, _hook__reference__illustration, _hook__reference__product_model)
FROM dab.bag__adventure_works__product_model_illustrations