MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__product_photo
  ),
  tags hook,
  grain (_pit_hook__reference__product_photo, _hook__reference__product_photo),
  description 'Hook viewpoint of product_photos data: Product images.',
  column_descriptions (
    product_photo__product_photo_id = 'Primary key for ProductPhoto records.',
    product_photo__thumb_nail_photo = 'Small image of the product.',
    product_photo__thumbnail_photo_file_name = 'Small image file name.',
    product_photo__large_photo = 'Large image of the product.',
    product_photo__large_photo_file_name = 'Large image file name.',
    product_photo__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_photo__record_updated_at = 'Timestamp when this record was last updated',
    product_photo__record_version = 'Version number for this record',
    product_photo__record_valid_from = 'Timestamp from which this record version is valid',
    product_photo__record_valid_to = 'Timestamp until which this record version is valid',
    product_photo__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__product_photo = 'Reference hook to product_photo reference',
    _pit_hook__reference__product_photo = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    product_photo_id AS product_photo__product_photo_id,
    thumb_nail_photo AS product_photo__thumb_nail_photo,
    thumbnail_photo_file_name AS product_photo__thumbnail_photo_file_name,
    large_photo AS product_photo__large_photo,
    large_photo_file_name AS product_photo__large_photo_file_name,
    modified_date AS product_photo__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_photo__record_loaded_at
  FROM das.raw__adventure_works__product_photos
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_photo__product_photo_id ORDER BY product_photo__record_loaded_at) AS product_photo__record_version,
    CASE
      WHEN product_photo__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_photo__record_loaded_at
    END AS product_photo__record_valid_from,
    COALESCE(
      LEAD(product_photo__record_loaded_at) OVER (PARTITION BY product_photo__product_photo_id ORDER BY product_photo__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_photo__record_valid_to,
    product_photo__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_photo__is_current_record,
    CASE
      WHEN product_photo__is_current_record
      THEN product_photo__record_loaded_at
      ELSE product_photo__record_valid_to
    END AS product_photo__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__product_photo__adventure_works|', product_photo__product_photo_id) AS _hook__reference__product_photo,
    CONCAT_WS('~',
      _hook__reference__product_photo,
      'epoch__valid_from|'||product_photo__record_valid_from
    ) AS _pit_hook__reference__product_photo,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__product_photo::BLOB,
  _hook__reference__product_photo::BLOB,
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
  product_photo__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND product_photo__record_updated_at BETWEEN @start_ts AND @end_ts