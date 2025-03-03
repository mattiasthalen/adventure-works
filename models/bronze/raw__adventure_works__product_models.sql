MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  product_model_id,
  catalog_description,
  instructions,
  modified_date,
  name,
  rowguid,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_models"
)
