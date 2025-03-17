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

SELECT
  *
  EXCLUDE (_hook__product_category)
FROM dab.bag__adventure_works__product_categories