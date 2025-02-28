MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column address__record_updated_at
  ),
  enabled FALSE
);

WITH staging AS (
  SELECT
    state_province_id,
    territory_id,
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
    ROW_NUMBER() OVER (PARTITION BY state_province_id ORDER BY state_province__record_loaded_at) AS state_province__record_version,
    CASE
      WHEN state_province__record_version = 1
      THEN @MIN_TS::TIMESTAMP
      ELSE state_province__record_loaded_at
    END AS state_province__record_valid_from,
    COALESCE(
      LEAD(state_province__record_loaded_at) OVER (PARTITION BY state_province_id ORDER BY state_province__record_loaded_at),
      @MAX_TS::TIMESTAMP
    ) AS state_province__record_valid_to,
    state_province__record_valid_to = @MAX_TS::TIMESTAMP AS state_province__is_current_record,
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
      state_province_id,
      '~epoch|valid_from|',
      state_province__record_valid_from
    )::BLOB AS _pit_hook__state_province,
    CONCAT('state_province|adventure_works|', state_province_id)::BLOB AS _hook__state_province,
    CONCAT('territory|adventure_works|', territory_id)::BLOB AS _hook__territory,
    *
  FROM validity
)
SELECT
  *
FROM hooks
WHERE 1 = 1
AND address__record_updated_at BETWEEN @start_ts AND @end_ts