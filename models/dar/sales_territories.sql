MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__territory__sales),
  description 'Business viewpoint of sales_territories data: Sales territory lookup table.',
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
    sales_territory__modified_date = 'Date when this record was last modified',
    sales_territory__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_territory__record_updated_at = 'Timestamp when this record was last updated',
    sales_territory__record_version = 'Version number for this record',
    sales_territory__record_valid_from = 'Timestamp from which this record version is valid',
    sales_territory__record_valid_to = 'Timestamp until which this record version is valid',
    sales_territory__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__territory__sales,
    sales_territory__territory_id,
    sales_territory__name,
    sales_territory__country_region_code,
    sales_territory__group,
    sales_territory__sales_ytd,
    sales_territory__sales_last_year,
    sales_territory__cost_ytd,
    sales_territory__cost_last_year,
    sales_territory__rowguid,
    sales_territory__modified_date,
    sales_territory__record_loaded_at,
    sales_territory__record_updated_at,
    sales_territory__record_version,
    sales_territory__record_valid_from,
    sales_territory__record_valid_to,
    sales_territory__is_current_record
  FROM dab.bag__adventure_works__sales_territories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__territory__sales,
    NULL AS sales_territory__territory_id,
    'N/A' AS sales_territory__name,
    'N/A' AS sales_territory__country_region_code,
    'N/A' AS sales_territory__group,
    NULL AS sales_territory__sales_ytd,
    NULL AS sales_territory__sales_last_year,
    NULL AS sales_territory__cost_ytd,
    NULL AS sales_territory__cost_last_year,
    'N/A' AS sales_territory__rowguid,
    NULL AS sales_territory__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_territory__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_territory__record_updated_at,
    0 AS sales_territory__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_territory__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS sales_territory__record_valid_to,
    TRUE AS sales_territory__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__territory__sales::BLOB,
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
  sales_territory__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.sales_territories TO './export/dar/sales_territories.parquet' (FORMAT parquet, COMPRESSION zstd)
);