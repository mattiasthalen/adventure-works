MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__country_region),
  description 'Business viewpoint of country_regions data: Lookup table containing the ISO standard codes for countries and regions.',
  column_descriptions (
    country_region__country_region_code = 'ISO standard code for countries and regions.',
    country_region__name = 'Country or region name.',
    country_region__modified_date = 'Date when this record was last modified',
    country_region__record_loaded_at = 'Timestamp when this record was loaded into the system',
    country_region__record_updated_at = 'Timestamp when this record was last updated',
    country_region__record_version = 'Version number for this record',
    country_region__record_valid_from = 'Timestamp from which this record version is valid',
    country_region__record_valid_to = 'Timestamp until which this record version is valid',
    country_region__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__country_region,
    country_region__country_region_code,
    country_region__name,
    country_region__modified_date,
    country_region__record_loaded_at,
    country_region__record_updated_at,
    country_region__record_version,
    country_region__record_valid_from,
    country_region__record_valid_to,
    country_region__is_current_record
  FROM dab.bag__adventure_works__country_regions
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__country_region,
    'N/A' AS country_region__country_region_code,
    'N/A' AS country_region__name,
    NULL AS country_region__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS country_region__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS country_region__record_updated_at,
    0 AS country_region__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS country_region__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS country_region__record_valid_to,
    TRUE AS country_region__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__country_region::BLOB,
  country_region__country_region_code::TEXT,
  country_region__name::TEXT,
  country_region__modified_date::DATE,
  country_region__record_loaded_at::TIMESTAMP,
  country_region__record_updated_at::TIMESTAMP,
  country_region__record_version::TEXT,
  country_region__record_valid_from::TIMESTAMP,
  country_region__record_valid_to::TIMESTAMP,
  country_region__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.country_regions TO './export/dar/country_regions.parquet' (FORMAT parquet, COMPRESSION zstd)
);