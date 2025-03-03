MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  state_province_id,
  territory_id,
  country_region_code,
  is_only_state_province_flag,
  modified_date,
  name,
  rowguid,
  state_province_code,
  _dlt_load_id
  FROM ICEBERG_SCAN(
    "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__state_provinces"
  )