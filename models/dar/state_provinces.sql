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

WITH cte__source AS (
  SELECT
    _pit_hook__reference__state_province,
    state_province__state_province_id,
    state_province__state_province_code,
    state_province__country_region_code,
    state_province__is_only_state_province_flag,
    state_province__name,
    state_province__territory_id,
    state_province__rowguid,
    state_province__modified_date,
    state_province__record_loaded_at,
    state_province__record_updated_at,
    state_province__record_version,
    state_province__record_valid_from,
    state_province__record_valid_to,
    state_province__is_current_record
  FROM dab.bag__adventure_works__state_provinces
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__state_province,
    NULL AS state_province__state_province_id,
    'N/A' AS state_province__state_province_code,
    'N/A' AS state_province__country_region_code,
    FALSE AS state_province__is_only_state_province_flag,
    'N/A' AS state_province__name,
    NULL AS state_province__territory_id,
    'N/A' AS state_province__rowguid,
    NULL AS state_province__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS state_province__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS state_province__record_updated_at,
    0 AS state_province__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS state_province__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS state_province__record_valid_to,
    TRUE AS state_province__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__state_province::BLOB,
  state_province__state_province_id::BIGINT,
  state_province__state_province_code::TEXT,
  state_province__country_region_code::TEXT,
  state_province__is_only_state_province_flag::BOOLEAN,
  state_province__name::TEXT,
  state_province__territory_id::BIGINT,
  state_province__rowguid::TEXT,
  state_province__modified_date::DATE,
  state_province__record_loaded_at::TIMESTAMP,
  state_province__record_updated_at::TIMESTAMP,
  state_province__record_version::TEXT,
  state_province__record_valid_from::TIMESTAMP,
  state_province__record_valid_to::TIMESTAMP,
  state_province__is_current_record::BOOLEAN
FROM cte__final