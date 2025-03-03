MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_review_id AS product_review__product_review_id,
    product_id AS product_review__product_id,
    comments AS product_review__comments,
    email_address AS product_review__email_address,
    modified_date AS product_review__modified_date,
    rating AS product_review__rating,
    review_date AS product_review__review_date,
    reviewer_name AS product_review__reviewer_name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_review__record_loaded_at
  FROM bronze.raw__adventure_works__product_reviews
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_review__product_review_id ORDER BY product_review__record_loaded_at) AS product_review__record_version,
    CASE
      WHEN product_review__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_review__record_loaded_at
    END AS product_review__record_valid_from,
    COALESCE(
      LEAD(product_review__record_loaded_at) OVER (PARTITION BY product_review__product_review_id ORDER BY product_review__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_review__record_valid_to,
    product_review__record_valid_to = @max_ts::TIMESTAMP AS product_review__is_current_record,
    CASE
      WHEN product_review__is_current_record
      THEN product_review__record_loaded_at
      ELSE product_review__record_valid_to
    END AS product_review__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product_review|adventure_works|',
      product_review__product_review_id,
      '~epoch|valid_from|',
      product_review__record_valid_from
    ) AS _pit_hook__product_review,
    CONCAT('product_review|adventure_works|', product_review__product_review_id) AS _hook__product_review,
    CONCAT('product|adventure_works|', product_review__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__product_review::BLOB,
  _hook__product_review::BLOB,
  _hook__product::BLOB,
  product_review__product_review_id::VARCHAR,
  product_review__product_id::VARCHAR,
  product_review__comments::VARCHAR,
  product_review__email_address::VARCHAR,
  product_review__modified_date::VARCHAR,
  product_review__rating::VARCHAR,
  product_review__review_date::VARCHAR,
  product_review__reviewer_name::VARCHAR,
  product_review__record_loaded_at::TIMESTAMP,
  product_review__record_version::INT,
  product_review__record_valid_from::TIMESTAMP,
  product_review__record_valid_to::TIMESTAMP,
  product_review__is_current_record::BOOLEAN,
  product_review__record_updated_at::TIMESTAMP
FROM hooks