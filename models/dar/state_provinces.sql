MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__state_province),
  description 'Business viewpoint of state_provinces data: State and province lookup table.',
  column_descriptions (
    state_province__state_province_id = 'Primary key for StateProvince records.',
    state_province__state_province_code = 'ISO standard state or province code.',
    state_province__country_region_code = 'ISO standard country or region code. Foreign key to CountryRegion.CountryRegionCode.',
    state_province__is_only_state_province_flag = '0 = StateProvinceCode exists. 1 = StateProvinceCode unavailable, using CountryRegionCode.',
    state_province__name = 'State or province description.',
    state_province__territory_id = 'ID of the territory in which the state or province is located. Foreign key to SalesTerritory.SalesTerritoryID.',
    state_province__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    state_province__modified_date = 'Date when this record was last modified',
    state_province__record_loaded_at = 'Timestamp when this record was loaded into the system',
    state_province__record_updated_at = 'Timestamp when this record was last updated',
    state_province__record_version = 'Version number for this record',
    state_province__record_valid_from = 'Timestamp from which this record version is valid',
    state_province__record_valid_to = 'Timestamp until which this record version is valid',
    state_province__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__state_province, _hook__reference__country_region, _hook__territory__sales)
FROM dab.bag__adventure_works__state_provinces