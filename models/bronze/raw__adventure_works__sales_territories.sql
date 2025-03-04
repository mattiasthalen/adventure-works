MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  territory_id::BIGINT,
  cost_last_year::DOUBLE,
  cost_ytd::DOUBLE,
  country_region_code::VARCHAR,
  group::VARCHAR,
  modified_date::VARCHAR,
  name::VARCHAR,
  rowguid::VARCHAR,
  sales_last_year::DOUBLE,
  sales_ytd::DOUBLE,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_territories"
);
