MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  illustration_id::BIGINT,
  product_model_id::BIGINT,
  modified_date::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_model_illustrations"
);
