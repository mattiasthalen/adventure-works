MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  location_id::BIGINT,
  product_id::BIGINT,
  bin::BIGINT,
  modified_date::VARCHAR,
  quantity::BIGINT,
  rowguid::VARCHAR,
  shelf::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_inventories"
);
