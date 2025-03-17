MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__country_region
  ),
  tags hook,
  grain (_pit_hook__reference__country_region, _hook__reference__country_region),
  description 'Hook viewpoint of country_regions data: Lookup table containing the ISO standard codes for countries and regions.',
  column_descriptions (
    country_region__country_region_code = 'ISO standard code for countries and regions.',
    country_region__name = 'Country or region name.',
    country_region__record_loaded_at = 'Timestamp when this record was loaded into the system',
    country_region__record_updated_at = 'Timestamp when this record was last updated',
    country_region__record_version = 'Version number for this record',
    country_region__record_valid_from = 'Timestamp from which this record version is valid',
    country_region__record_valid_to = 'Timestamp until which this record version is valid',
    country_region__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__country_region = 'Reference hook to country_region reference',
    _pit_hook__reference__country_region = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    country_region_code AS country_region__country_region_code,
    name AS country_region__name,
    modified_date AS country_region__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS country_region__record_loaded_at
  FROM das.raw__adventure_works__country_regions
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country_region__country_region_code ORDER BY country_region__record_loaded_at) AS country_region__record_version,
    CASE
      WHEN country_region__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE country_region__record_loaded_at
    END AS country_region__record_valid_from,
    COALESCE(
      LEAD(country_region__record_loaded_at) OVER (PARTITION BY country_region__country_region_code ORDER BY country_region__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS country_region__record_valid_to,
    country_region__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS country_region__is_current_record,
    CASE
      WHEN country_region__is_current_record
      THEN country_region__record_loaded_at
      ELSE country_region__record_valid_to
    END AS country_region__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__country_region__adventure_works|', country_region__country_region_code) AS _hook__reference__country_region,
    CONCAT_WS('~',
      _hook__reference__country_region,
      'epoch__valid_from|'||country_region__record_valid_from
    ) AS _pit_hook__reference__country_region,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__country_region::BLOB,
  _hook__reference__country_region::BLOB,
  country_region__country_region_code::TEXT,
  country_region__name::TEXT,
  country_region__modified_date::DATE,
  country_region__record_loaded_at::TIMESTAMP,
  country_region__record_updated_at::TIMESTAMP,
  country_region__record_version::TEXT,
  country_region__record_valid_from::TIMESTAMP,
  country_region__record_valid_to::TIMESTAMP,
  country_region__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND country_region__record_updated_at BETWEEN @start_ts AND @end_ts