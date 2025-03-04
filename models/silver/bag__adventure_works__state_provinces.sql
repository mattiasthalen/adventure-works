MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column state_provinc__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    state_province_id AS state_provinc__state_province_id,
    territory_id AS state_provinc__territory_id,
    country_region_code AS state_provinc__country_region_code,
    is_only_state_province_flag AS state_provinc__is_only_state_province_flag,
    modified_date AS state_provinc__modified_date,
    name AS state_provinc__name,
    rowguid AS state_provinc__rowguid,
    state_province_code AS state_provinc__state_province_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS state_provinc__record_loaded_at
  FROM bronze.raw__adventure_works__state_provinces
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY state_provinc__state_province_id ORDER BY state_provinc__record_loaded_at) AS state_provinc__record_version,
    CASE
      WHEN state_provinc__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE state_provinc__record_loaded_at
    END AS state_provinc__record_valid_from,
    COALESCE(
      LEAD(state_provinc__record_loaded_at) OVER (PARTITION BY state_provinc__state_province_id ORDER BY state_provinc__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS state_provinc__record_valid_to,
    state_provinc__record_valid_to = @max_ts::TIMESTAMP AS state_provinc__is_current_record,
    CASE
      WHEN state_provinc__is_current_record
      THEN state_provinc__record_loaded_at
      ELSE state_provinc__record_valid_to
    END AS state_provinc__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'state_province|adventure_works|',
      state_provinc__state_province_id,
      '~epoch|valid_from|',
      state_provinc__record_valid_from
    ) AS _pit_hook__state_province,
    CONCAT('state_province|adventure_works|', state_provinc__state_province_id) AS _hook__state_province,
    CONCAT('territory|adventure_works|', state_provinc__territory_id) AS _hook__territory,
    *
  FROM validity
)
SELECT
  _pit_hook__state_province::BLOB,
  _hook__state_province::BLOB,
  _hook__territory::BLOB,
  state_provinc__state_province_id::BIGINT,
  state_provinc__territory_id::BIGINT,
  state_provinc__country_region_code::VARCHAR,
  state_provinc__is_only_state_province_flag::BOOLEAN,
  state_provinc__modified_date::VARCHAR,
  state_provinc__name::VARCHAR,
  state_provinc__rowguid::VARCHAR,
  state_provinc__state_province_code::VARCHAR,
  state_provinc__record_loaded_at::TIMESTAMP,
  state_provinc__record_updated_at::TIMESTAMP,
  state_provinc__record_valid_from::TIMESTAMP,
  state_provinc__record_valid_to::TIMESTAMP,
  state_provinc__record_version::INT,
  state_provinc__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND state_provinc__record_updated_at BETWEEN @start_ts AND @end_ts