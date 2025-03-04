MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_photo__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_photo_id AS product_photo__product_photo_id,
    large_photo AS product_photo__large_photo,
    large_photo_file_name AS product_photo__large_photo_file_name,
    modified_date AS product_photo__modified_date,
    thumb_nail_photo AS product_photo__thumb_nail_photo,
    thumbnail_photo_file_name AS product_photo__thumbnail_photo_file_name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_photo__record_loaded_at
  FROM bronze.raw__adventure_works__product_photos
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_photo__product_photo_id ORDER BY product_photo__record_loaded_at) AS product_photo__record_version,
    CASE
      WHEN product_photo__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_photo__record_loaded_at
    END AS product_photo__record_valid_from,
    COALESCE(
      LEAD(product_photo__record_loaded_at) OVER (PARTITION BY product_photo__product_photo_id ORDER BY product_photo__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_photo__record_valid_to,
    product_photo__record_valid_to = @max_ts::TIMESTAMP AS product_photo__is_current_record,
    CASE
      WHEN product_photo__is_current_record
      THEN product_photo__record_loaded_at
      ELSE product_photo__record_valid_to
    END AS product_photo__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product_photo|adventure_works|',
      product_photo__product_photo_id,
      '~epoch|valid_from|',
      product_photo__record_valid_from
    ) AS _pit_hook__product_photo,
    CONCAT('product_photo|adventure_works|', product_photo__product_photo_id) AS _hook__product_photo,
    *
  FROM validity
)
SELECT
  _pit_hook__product_photo::BLOB,
  _hook__product_photo::BLOB,
  product_photo__product_photo_id::BIGINT,
  product_photo__large_photo::VARCHAR,
  product_photo__large_photo_file_name::VARCHAR,
  product_photo__modified_date::VARCHAR,
  product_photo__thumb_nail_photo::VARCHAR,
  product_photo__thumbnail_photo_file_name::VARCHAR,
  product_photo__record_loaded_at::TIMESTAMP,
  product_photo__record_updated_at::TIMESTAMP,
  product_photo__record_valid_from::TIMESTAMP,
  product_photo__record_valid_to::TIMESTAMP,
  product_photo__record_version::INT,
  product_photo__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND product_photo__record_updated_at BETWEEN @start_ts AND @end_ts