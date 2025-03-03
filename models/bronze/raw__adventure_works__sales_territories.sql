MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  territory_id,
  cost_last_year,
  cost_ytd,
  country_region_code,
  group,
  modified_date,
  name,
  rowguid,
  sales_last_year,
  sales_ytd,
  _dlt_load_id
  FROM ICEBERG_SCAN(
    "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_territories"
  )