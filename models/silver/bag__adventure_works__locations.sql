MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    location_id AS location__location_id,
    availability AS location__availability,
    cost_rate AS location__cost_rate,
    modified_date AS location__modified_date,
    name AS location__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS location__record_loaded_at
  FROM bronze.raw__adventure_works__locations
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY location__location_id ORDER BY location__record_loaded_at) AS location__record_version,
    CASE
      WHEN location__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE location__record_loaded_at
    END AS location__record_valid_from,
    COALESCE(
      LEAD(location__record_loaded_at) OVER (PARTITION BY location__location_id ORDER BY location__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS location__record_valid_to,
    location__record_valid_to = @max_ts::TIMESTAMP AS location__is_current_record,
    CASE
      WHEN location__is_current_record
      THEN location__record_loaded_at
      ELSE location__record_valid_to
    END AS location__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'location|adventure_works|',
      location__location_id,
      '~epoch|valid_from|',
      location__record_valid_from
    ) AS _pit_hook__location,
    CONCAT('location|adventure_works|', location__location_id) AS _hook__location,
    *
  FROM validity
)
SELECT
  _pit_hook__location::BLOB,
  _hook__location::BLOB,
  location__location_id::VARCHAR,
  location__availability::VARCHAR,
  location__cost_rate::VARCHAR,
  location__modified_date::VARCHAR,
  location__name::VARCHAR,
  location__record_loaded_at::TIMESTAMP,
  location__record_version::INT,
  location__record_valid_from::TIMESTAMP,
  location__record_valid_to::TIMESTAMP,
  location__is_current_record::BOOLEAN,
  location__record_updated_at::TIMESTAMP
FROM hooks