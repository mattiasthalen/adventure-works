MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  address_type_id::BIGINT,
  modified_date::VARCHAR,
  name::VARCHAR,
  rowguid::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__address_types"
);
