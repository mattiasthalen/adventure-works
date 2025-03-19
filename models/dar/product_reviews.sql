MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_review),
  description 'Business viewpoint of product_reviews data: Customer reviews of products they have purchased.',
  column_descriptions (
    product_review__product_review_id = 'Primary key for ProductReview records.',
    product_review__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    product_review__reviewer_name = 'Name of the reviewer.',
    product_review__review_date = 'Date review was submitted.',
    product_review__email_address = 'Reviewer''s e-mail address.',
    product_review__rating = 'Product rating given by the reviewer. Scale is 1 to 5 with 5 as the highest rating.',
    product_review__comments = 'Reviewer''s comments.',
    product_review__modified_date = 'Date when this record was last modified',
    product_review__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_review__record_updated_at = 'Timestamp when this record was last updated',
    product_review__record_version = 'Version number for this record',
    product_review__record_valid_from = 'Timestamp from which this record version is valid',
    product_review__record_valid_to = 'Timestamp until which this record version is valid',
    product_review__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__product_review,
    product_review__product_review_id,
    product_review__product_id,
    product_review__reviewer_name,
    product_review__review_date,
    product_review__email_address,
    product_review__rating,
    product_review__comments,
    product_review__modified_date,
    product_review__record_loaded_at,
    product_review__record_updated_at,
    product_review__record_version,
    product_review__record_valid_from,
    product_review__record_valid_to,
    product_review__is_current_record
  FROM dab.bag__adventure_works__product_reviews
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__product_review,
    NULL AS product_review__product_review_id,
    NULL AS product_review__product_id,
    'N/A' AS product_review__reviewer_name,
    NULL AS product_review__review_date,
    'N/A' AS product_review__email_address,
    NULL AS product_review__rating,
    'N/A' AS product_review__comments,
    NULL AS product_review__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_review__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_review__record_updated_at,
    0 AS product_review__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_review__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_review__record_valid_to,
    TRUE AS product_review__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__product_review::BLOB,
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
  product_review__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.product_reviews TO './export/dar/product_reviews.parquet' (FORMAT parquet, COMPRESSION zstd)
);