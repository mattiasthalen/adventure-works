MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    state_province_id AS state_province__state_province_id,
    territory_id AS state_province__territory_id,
    country_region_code AS state_province__country_region_code,
    is_only_state_province_flag AS state_province__is_only_state_province_flag,
    modified_date AS state_province__modified_date,
    name AS state_province__name,
    rowguid AS state_province__rowguid,
    state_province_code AS state_province__state_province_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS state_province__record_loaded_at
  FROM bronze.raw__adventure_works__state_provinces
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY state_province__state_province_id ORDER BY state_province__record_loaded_at) AS state_province__record_version,
    CASE
      WHEN state_province__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE state_province__record_loaded_at
    END AS state_province__record_valid_from,
    COALESCE(
      LEAD(state_province__record_loaded_at) OVER (PARTITION BY state_province__state_province_id ORDER BY state_province__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS state_province__record_valid_to,
    state_province__record_valid_to = @max_ts::TIMESTAMP AS state_province__is_current_record,
    CASE
      WHEN state_province__is_current_record
      THEN state_province__record_loaded_at
      ELSE state_province__record_valid_to
    END AS state_province__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'state_province|adventure_works|',
      state_province__state_province_id,
      '~epoch|valid_from|',
      state_province__record_valid_from
    ) AS _pit_hook__state_province,
    CONCAT('state_province|adventure_works|', state_province__state_province_id) AS _hook__state_province,
    CONCAT('territory|adventure_works|', state_province__territory_id) AS _hook__territory,
    *
  FROM validity
)
SELECT
  _pit_hook__state_province::BLOB,
  _hook__state_province::BLOB,
  _hook__territory::BLOB,
  state_province__state_province_id::VARCHAR,
  state_province__territory_id::VARCHAR,
  state_province__country_region_code::VARCHAR,
  state_province__is_only_state_province_flag::VARCHAR,
  state_province__modified_date::VARCHAR,
  state_province__name::VARCHAR,
  state_province__rowguid::VARCHAR,
  state_province__state_province_code::VARCHAR,
  state_province__record_loaded_at::TIMESTAMP,
  state_province__record_version::INT,
  state_province__record_valid_from::TIMESTAMP,
  state_province__record_valid_to::TIMESTAMP,
  state_province__is_current_record::BOOLEAN,
  state_province__record_updated_at::TIMESTAMP
FROM hooks