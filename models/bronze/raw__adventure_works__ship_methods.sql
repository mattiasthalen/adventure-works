MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  ship_method_id::BIGINT,
  modified_date::VARCHAR,
  name::VARCHAR,
  rowguid::VARCHAR,
  ship_base::DOUBLE,
  ship_rate::DOUBLE,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__ship_methods"
);
