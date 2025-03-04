MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  product_review_id::BIGINT,
  product_id::BIGINT,
  comments::VARCHAR,
  email_address::VARCHAR,
  modified_date::VARCHAR,
  rating::BIGINT,
  review_date::VARCHAR,
  reviewer_name::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_reviews"
);
