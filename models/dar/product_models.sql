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

SELECT
  *
  EXCLUDE (_hook__reference__product_model)
FROM dab.bag__adventure_works__product_models