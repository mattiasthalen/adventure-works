MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__product_photo,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__reference__product_photo, _hook__reference__product_photo)
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
  FROM bronze.raw__adventure_works__product_photos
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
    CONCAT(
      'reference__product_photo__adventure_works|',
      product_photo__product_photo_id,
      '~epoch|valid_from|',
      product_photo__record_valid_from
    )::BLOB AS _pit_hook__reference__product_photo,
    CONCAT('reference__product_photo__adventure_works|', product_photo__product_photo_id) AS _hook__reference__product_photo,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__product_photo::BLOB,
  _hook__reference__product_photo::BLOB,
  product_photo__product_photo_id::BIGINT,
  product_photo__thumb_nail_photo::TEXT,
  product_photo__thumbnail_photo_file_name::TEXT,
  product_photo__large_photo::TEXT,
  product_photo__large_photo_file_name::TEXT,
  product_photo__modified_date::DATE,
  product_photo__record_loaded_at::TIMESTAMP,
  product_photo__record_updated_at::TIMESTAMP,
  product_photo__record_version::TEXT,
  product_photo__record_valid_from::TIMESTAMP,
  product_photo__record_valid_to::TIMESTAMP,
  product_photo__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND product_photo__record_updated_at BETWEEN @start_ts AND @end_ts