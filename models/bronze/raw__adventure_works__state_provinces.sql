MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  state_province_id::BIGINT,
  territory_id::BIGINT,
  country_region_code::VARCHAR,
  is_only_state_province_flag::BOOLEAN,
  modified_date::VARCHAR,
  name::VARCHAR,
  rowguid::VARCHAR,
  state_province_code::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__state_provinces"
);
