MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of sales_territories data: Sales territory lookup table.',
  column_descriptions (
    territory_id = 'Primary key for SalesTerritory records.',
    name = 'Sales territory description.',
    country_region_code = 'ISO standard country or region code. Foreign key to CountryRegion.CountryRegionCode.',
    group = 'Geographic area to which the sales territory belongs.',
    sales_ytd = 'Sales in the territory year to date.',
    sales_last_year = 'Sales in the territory the previous year.',
    cost_ytd = 'Business costs in the territory year to date.',
    cost_last_year = 'Business costs in the territory the previous year.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    territory_id::BIGINT,
    name::TEXT,
    country_region_code::TEXT,
    group::TEXT,
    sales_ytd::DOUBLE,
    sales_last_year::DOUBLE,
    cost_ytd::DOUBLE,
    cost_last_year::DOUBLE,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__sales_territories"
)
;