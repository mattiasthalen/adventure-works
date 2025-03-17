MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of state_provinces data: State and province lookup table.',
  column_descriptions (
    state_province_id = 'Primary key for StateProvince records.',
    state_province_code = 'ISO standard state or province code.',
    country_region_code = 'ISO standard country or region code. Foreign key to CountryRegion.CountryRegionCode.',
    is_only_state_province_flag = '0 = StateProvinceCode exists. 1 = StateProvinceCode unavailable, using CountryRegionCode.',
    name = 'State or province description.',
    territory_id = 'ID of the territory in which the state or province is located. Foreign key to SalesTerritory.SalesTerritoryID.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    state_province_id::BIGINT,
    state_province_code::TEXT,
    country_region_code::TEXT,
    is_only_state_province_flag::BOOLEAN,
    name::TEXT,
    territory_id::BIGINT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__state_provinces"
)
;