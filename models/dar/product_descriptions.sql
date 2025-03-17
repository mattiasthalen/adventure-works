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

SELECT
  *
  EXCLUDE (_hook__reference__product_description)
FROM dab.bag__adventure_works__product_descriptions