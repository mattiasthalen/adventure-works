MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__state_province
  ),
  tags hook,
  grain (_pit_hook__reference__state_province, _hook__reference__state_province),
  description 'Hook viewpoint of state_provinces data: State and province lookup table.',
  references (_hook__reference__country_region, _hook__territory__sales),
  column_descriptions (
    state_province__state_province_id = 'Primary key for StateProvince records.',
    state_province__state_province_code = 'ISO standard state or province code.',
    state_province__country_region_code = 'ISO standard country or region code. Foreign key to CountryRegion.CountryRegionCode.',
    state_province__is_only_state_province_flag = '0 = StateProvinceCode exists. 1 = StateProvinceCode unavailable, using CountryRegionCode.',
    state_province__name = 'State or province description.',
    state_province__territory_id = 'ID of the territory in which the state or province is located. Foreign key to SalesTerritory.SalesTerritoryID.',
    state_province__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    state_province__record_loaded_at = 'Timestamp when this record was loaded into the system',
    state_province__record_updated_at = 'Timestamp when this record was last updated',
    state_province__record_version = 'Version number for this record',
    state_province__record_valid_from = 'Timestamp from which this record version is valid',
    state_province__record_valid_to = 'Timestamp until which this record version is valid',
    state_province__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__state_province = 'Reference hook to state_province reference',
    _hook__reference__country_region = 'Reference hook to country_region reference',
    _hook__territory__sales = 'Reference hook to sales territory',
    _pit_hook__reference__state_province = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    state_province_id AS state_province__state_province_id,
    state_province_code AS state_province__state_province_code,
    country_region_code AS state_province__country_region_code,
    is_only_state_province_flag AS state_province__is_only_state_province_flag,
    name AS state_province__name,
    territory_id AS state_province__territory_id,
    rowguid AS state_province__rowguid,
    modified_date AS state_province__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS state_province__record_loaded_at
  FROM das.raw__adventure_works__state_provinces
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY state_province__state_province_id ORDER BY state_province__record_loaded_at) AS state_province__record_version,
    CASE
      WHEN state_province__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE state_province__record_loaded_at
    END AS state_province__record_valid_from,
    COALESCE(
      LEAD(state_province__record_loaded_at) OVER (PARTITION BY state_province__state_province_id ORDER BY state_province__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS state_province__record_valid_to,
    state_province__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS state_province__is_current_record,
    CASE
      WHEN state_province__is_current_record
      THEN state_province__record_loaded_at
      ELSE state_province__record_valid_to
    END AS state_province__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__state_province__adventure_works|', state_province__state_province_id) AS _hook__reference__state_province,
    CONCAT('reference__country_region__adventure_works|', state_province__country_region_code) AS _hook__reference__country_region,
    CONCAT('territory__sales__adventure_works|', state_province__territory_id) AS _hook__territory__sales,
    CONCAT_WS('~',
      _hook__reference__state_province,
      'epoch__valid_from|'||state_province__record_valid_from
    ) AS _pit_hook__reference__state_province,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__state_province::BLOB,
  _hook__reference__state_province::BLOB,
  _hook__reference__country_region::BLOB,
  _hook__territory__sales::BLOB,
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
  state_province__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND state_province__record_updated_at BETWEEN @start_ts AND @end_ts