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

SELECT
  *
  EXCLUDE (_hook__product_review, _hook__product)
FROM dab.bag__adventure_works__product_reviews