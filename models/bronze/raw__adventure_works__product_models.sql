MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  product_model_id::BIGINT,
  catalog_description::VARCHAR,
  instructions::VARCHAR,
  modified_date::VARCHAR,
  name::VARCHAR,
  rowguid::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_models"
);
