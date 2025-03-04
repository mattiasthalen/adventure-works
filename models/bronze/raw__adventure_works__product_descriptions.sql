MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  product_description_id::BIGINT,
  description::VARCHAR,
  modified_date::VARCHAR,
  rowguid::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_descriptions"
);
