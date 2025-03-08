MODEL (
  kind VIEW,
  enabled TRUE
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
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_reviews"
)
;