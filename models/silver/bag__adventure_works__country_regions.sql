MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column country_region__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    country_region_code AS country_region__country_region_code,
    modified_date AS country_region__modified_date,
    name AS country_region__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS country_region__record_loaded_at
  FROM bronze.raw__adventure_works__country_regions
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country_region__country_region_code ORDER BY country_region__record_loaded_at) AS country_region__record_version,
    CASE
      WHEN country_region__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE country_region__record_loaded_at
    END AS country_region__record_valid_from,
    COALESCE(
      LEAD(country_region__record_loaded_at) OVER (PARTITION BY country_region__country_region_code ORDER BY country_region__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS country_region__record_valid_to,
    country_region__record_valid_to = @max_ts::TIMESTAMP AS country_region__is_current_record,
    CASE
      WHEN country_region__is_current_record
      THEN country_region__record_loaded_at
      ELSE country_region__record_valid_to
    END AS country_region__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'country_region_code|adventure_works|',
      country_region__country_region_code,
      '~epoch|valid_from|',
      country_region__record_valid_from
    ) AS _pit_hook__country_region_code,
    CONCAT('country_region_code|adventure_works|', country_region__country_region_code) AS _hook__country_region_code,
    *
  FROM validity
)
SELECT
  _pit_hook__country_region_code::BLOB,
  _hook__country_region_code::BLOB,
  country_region__country_region_code::VARCHAR,
  country_region__modified_date::VARCHAR,
  country_region__name::VARCHAR,
  country_region__record_loaded_at::TIMESTAMP,
  country_region__record_updated_at::TIMESTAMP,
  country_region__record_valid_from::TIMESTAMP,
  country_region__record_valid_to::TIMESTAMP,
  country_region__record_version::INT,
  country_region__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND country_region__record_updated_at BETWEEN @start_ts AND @end_ts