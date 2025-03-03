MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  illustration_id,
  modified_date,
  product_model_id,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_model_illustrations"
)
