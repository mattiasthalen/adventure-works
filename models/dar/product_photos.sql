MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__product_photo),
  description 'Business viewpoint of product_photos data: Product images.',
  column_descriptions (
    product_photo__product_photo_id = 'Primary key for ProductPhoto records.',
    product_photo__thumb_nail_photo = 'Small image of the product.',
    product_photo__thumbnail_photo_file_name = 'Small image file name.',
    product_photo__large_photo = 'Large image of the product.',
    product_photo__large_photo_file_name = 'Large image file name.',
    product_photo__modified_date = 'Date when this record was last modified',
    product_photo__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_photo__record_updated_at = 'Timestamp when this record was last updated',
    product_photo__record_version = 'Version number for this record',
    product_photo__record_valid_from = 'Timestamp from which this record version is valid',
    product_photo__record_valid_to = 'Timestamp until which this record version is valid',
    product_photo__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__product_photo)
FROM dab.bag__adventure_works__product_photos