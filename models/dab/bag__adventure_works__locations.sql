MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__location
  ),
  tags hook,
  grain (_pit_hook__reference__location, _hook__reference__location)
);

WITH staging AS (
  SELECT
    location_id AS location__location_id,
    name AS location__name,
    cost_rate AS location__cost_rate,
    availability AS location__availability,
    modified_date AS location__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS location__record_loaded_at
  FROM das.raw__adventure_works__locations
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY location__location_id ORDER BY location__record_loaded_at) AS location__record_version,
    CASE
      WHEN location__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE location__record_loaded_at
    END AS location__record_valid_from,
    COALESCE(
      LEAD(location__record_loaded_at) OVER (PARTITION BY location__location_id ORDER BY location__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS location__record_valid_to,
    location__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS location__is_current_record,
    CASE
      WHEN location__is_current_record
      THEN location__record_loaded_at
      ELSE location__record_valid_to
    END AS location__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__location__adventure_works|', location__location_id) AS _hook__reference__location,
    CONCAT_WS('~',
      _hook__reference__location,
      'epoch__valid_from|'||location__record_valid_from
    ) AS _pit_hook__reference__location,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__location::BLOB,
  _hook__reference__location::BLOB,
  location__location_id::BIGINT,
  location__name::TEXT,
  location__cost_rate::DOUBLE,
  location__availability::DOUBLE,
  location__modified_date::DATE,
  location__record_loaded_at::TIMESTAMP,
  location__record_updated_at::TIMESTAMP,
  location__record_version::TEXT,
  location__record_valid_from::TIMESTAMP,
  location__record_valid_to::TIMESTAMP,
  location__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND location__record_updated_at BETWEEN @start_ts AND @end_ts