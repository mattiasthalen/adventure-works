MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  country_region_code::VARCHAR,
  modified_date::VARCHAR,
  name::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__country_regions"
);
