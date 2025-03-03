MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  product_review_id,
  comments,
  email_address,
  modified_date,
  product_id,
  rating,
  review_date,
  reviewer_name,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_reviews"
)
