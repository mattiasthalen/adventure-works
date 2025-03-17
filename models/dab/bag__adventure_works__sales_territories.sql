MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__territory__sales
  ),
  tags hook,
  grain (_pit_hook__territory__sales, _hook__territory__sales),
  description 'Hook viewpoint of sales_territories data: Sales territory lookup table.',
  references (_hook__reference__country_region),
  column_descriptions (
    sales_territory__territory_id = 'Primary key for SalesTerritory records.',
    sales_territory__name = 'Sales territory description.',
    sales_territory__country_region_code = 'ISO standard country or region code. Foreign key to CountryRegion.CountryRegionCode.',
    sales_territory__group = 'Geographic area to which the sales territory belongs.',
    sales_territory__sales_ytd = 'Sales in the territory year to date.',
    sales_territory__sales_last_year = 'Sales in the territory the previous year.',
    sales_territory__cost_ytd = 'Business costs in the territory year to date.',
    sales_territory__cost_last_year = 'Business costs in the territory the previous year.',
    sales_territory__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_territory__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_territory__record_updated_at = 'Timestamp when this record was last updated',
    sales_territory__record_version = 'Version number for this record',
    sales_territory__record_valid_from = 'Timestamp from which this record version is valid',
    sales_territory__record_valid_to = 'Timestamp until which this record version is valid',
    sales_territory__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__territory__sales = 'Reference hook to sales territory',
    _hook__reference__country_region = 'Reference hook to country_region reference',
    _pit_hook__territory__sales = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    territory_id AS sales_territory__territory_id,
    name AS sales_territory__name,
    country_region_code AS sales_territory__country_region_code,
    group AS sales_territory__group,
    sales_ytd AS sales_territory__sales_ytd,
    sales_last_year AS sales_territory__sales_last_year,
    cost_ytd AS sales_territory__cost_ytd,
    cost_last_year AS sales_territory__cost_last_year,
    rowguid AS sales_territory__rowguid,
    modified_date AS sales_territory__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_territory__record_loaded_at
  FROM das.raw__adventure_works__sales_territories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_territory__territory_id ORDER BY sales_territory__record_loaded_at) AS sales_territory__record_version,
    CASE
      WHEN sales_territory__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_territory__record_loaded_at
    END AS sales_territory__record_valid_from,
    COALESCE(
      LEAD(sales_territory__record_loaded_at) OVER (PARTITION BY sales_territory__territory_id ORDER BY sales_territory__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_territory__record_valid_to,
    sales_territory__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_territory__is_current_record,
    CASE
      WHEN sales_territory__is_current_record
      THEN sales_territory__record_loaded_at
      ELSE sales_territory__record_valid_to
    END AS sales_territory__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('territory__sales__adventure_works|', sales_territory__territory_id) AS _hook__territory__sales,
    CONCAT('reference__country_region__adventure_works|', sales_territory__country_region_code) AS _hook__reference__country_region,
    CONCAT_WS('~',
      _hook__territory__sales,
      'epoch__valid_from|'||sales_territory__record_valid_from
    ) AS _pit_hook__territory__sales,
    *
  FROM validity
)
SELECT
  _pit_hook__territory__sales::BLOB,
  _hook__territory__sales::BLOB,
  _hook__reference__country_region::BLOB,
  sales_territory__territory_id::BIGINT,
  sales_territory__name::TEXT,
  sales_territory__country_region_code::TEXT,
  sales_territory__group::TEXT,
  sales_territory__sales_ytd::DOUBLE,
  sales_territory__sales_last_year::DOUBLE,
  sales_territory__cost_ytd::DOUBLE,
  sales_territory__cost_last_year::DOUBLE,
  sales_territory__rowguid::TEXT,
  sales_territory__modified_date::DATE,
  sales_territory__record_loaded_at::TIMESTAMP,
  sales_territory__record_updated_at::TIMESTAMP,
  sales_territory__record_version::TEXT,
  sales_territory__record_valid_from::TIMESTAMP,
  sales_territory__record_valid_to::TIMESTAMP,
  sales_territory__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND sales_territory__record_updated_at BETWEEN @start_ts AND @end_ts