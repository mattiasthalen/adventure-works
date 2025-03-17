MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_reviews data: Customer reviews of products they have purchased.',
  column_descriptions (
    product_review_id = 'Primary key for ProductReview records.',
    product_id = 'Product identification number. Foreign key to Product.ProductID.',
    reviewer_name = 'Name of the reviewer.',
    review_date = 'Date review was submitted.',
    email_address = 'Reviewer''s e-mail address.',
    rating = 'Product rating given by the reviewer. Scale is 1 to 5 with 5 as the highest rating.',
    comments = 'Reviewer''s comments.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    product_review_id::BIGINT,
    product_id::BIGINT,
    reviewer_name::TEXT,
    review_date::DATE,
    email_address::TEXT,
    rating::BIGINT,
    comments::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_reviews"
)
;