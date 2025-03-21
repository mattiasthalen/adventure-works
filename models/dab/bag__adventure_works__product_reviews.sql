MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__product_review
  ),
  tags hook,
  grain (_pit_hook__product_review, _hook__product_review),
  description 'Hook viewpoint of product_reviews data: Customer reviews of products they have purchased.',
  references (_hook__product),
  column_descriptions (
    product_review__product_review_id = 'Primary key for ProductReview records.',
    product_review__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    product_review__reviewer_name = 'Name of the reviewer.',
    product_review__review_date = 'Date review was submitted.',
    product_review__email_address = 'Reviewer''s e-mail address.',
    product_review__rating = 'Product rating given by the reviewer. Scale is 1 to 5 with 5 as the highest rating.',
    product_review__comments = 'Reviewer''s comments.',
    product_review__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_review__record_updated_at = 'Timestamp when this record was last updated',
    product_review__record_version = 'Version number for this record',
    product_review__record_valid_from = 'Timestamp from which this record version is valid',
    product_review__record_valid_to = 'Timestamp until which this record version is valid',
    product_review__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__product_review = 'Reference hook to product_review',
    _hook__product = 'Reference hook to product',
    _pit_hook__product_review = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    product_review_id AS product_review__product_review_id,
    product_id AS product_review__product_id,
    reviewer_name AS product_review__reviewer_name,
    review_date AS product_review__review_date,
    email_address AS product_review__email_address,
    rating AS product_review__rating,
    comments AS product_review__comments,
    modified_date AS product_review__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_review__record_loaded_at
  FROM das.raw__adventure_works__product_reviews
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_review__product_review_id ORDER BY product_review__record_loaded_at) AS product_review__record_version,
    CASE
      WHEN product_review__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_review__record_loaded_at
    END AS product_review__record_valid_from,
    COALESCE(
      LEAD(product_review__record_loaded_at) OVER (PARTITION BY product_review__product_review_id ORDER BY product_review__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_review__record_valid_to,
    product_review__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_review__is_current_record,
    CASE
      WHEN product_review__is_current_record
      THEN product_review__record_loaded_at
      ELSE product_review__record_valid_to
    END AS product_review__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product__adventure_works|', product_review__product_review_id) AS _hook__product_review,
    CONCAT('product__adventure_works|', product_review__product_id) AS _hook__product,
    CONCAT_WS('~',
      _hook__product_review,
      'epoch__valid_from|'||product_review__record_valid_from
    ) AS _pit_hook__product_review,
    *
  FROM validity
)
SELECT
  _pit_hook__product_review::BLOB,
  _hook__product_review::BLOB,
  _hook__product::BLOB,
  product_review__product_review_id::BIGINT,
  product_review__product_id::BIGINT,
  product_review__reviewer_name::TEXT,
  product_review__review_date::DATE,
  product_review__email_address::TEXT,
  product_review__rating::BIGINT,
  product_review__comments::TEXT,
  product_review__modified_date::DATE,
  product_review__record_loaded_at::TIMESTAMP,
  product_review__record_updated_at::TIMESTAMP,
  product_review__record_version::TEXT,
  product_review__record_valid_from::TIMESTAMP,
  product_review__record_valid_to::TIMESTAMP,
  product_review__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND product_review__record_updated_at BETWEEN @start_ts AND @end_ts