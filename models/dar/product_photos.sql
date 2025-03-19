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

WITH cte__source AS (
  SELECT
    _pit_hook__reference__product_photo,
    product_photo__product_photo_id,
    product_photo__thumb_nail_photo,
    product_photo__thumbnail_photo_file_name,
    product_photo__large_photo,
    product_photo__large_photo_file_name,
    product_photo__modified_date,
    product_photo__record_loaded_at,
    product_photo__record_updated_at,
    product_photo__record_version,
    product_photo__record_valid_from,
    product_photo__record_valid_to,
    product_photo__is_current_record
  FROM dab.bag__adventure_works__product_photos
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__product_photo,
    NULL AS product_photo__product_photo_id,
    NULL AS product_photo__thumb_nail_photo,
    'N/A' AS product_photo__thumbnail_photo_file_name,
    NULL AS product_photo__large_photo,
    'N/A' AS product_photo__large_photo_file_name,
    NULL AS product_photo__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_photo__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_photo__record_updated_at,
    0 AS product_photo__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_photo__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_photo__record_valid_to,
    TRUE AS product_photo__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__product_photo::BLOB,
  product_photo__product_photo_id::BIGINT,
  product_photo__thumb_nail_photo::BINARY,
  product_photo__thumbnail_photo_file_name::TEXT,
  product_photo__large_photo::BINARY,
  product_photo__large_photo_file_name::TEXT,
  product_photo__modified_date::DATE,
  product_photo__record_loaded_at::TIMESTAMP,
  product_photo__record_updated_at::TIMESTAMP,
  product_photo__record_version::TEXT,
  product_photo__record_valid_from::TIMESTAMP,
  product_photo__record_valid_to::TIMESTAMP,
  product_photo__is_current_record::BOOLEAN
FROM cte__final