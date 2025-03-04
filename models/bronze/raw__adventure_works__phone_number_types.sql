MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  phone_number_type_id::BIGINT,
  modified_date::VARCHAR,
  name::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__phone_number_types"
);
