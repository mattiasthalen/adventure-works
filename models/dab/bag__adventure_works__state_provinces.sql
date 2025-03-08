MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__state_province,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__reference__state_province, _hook__reference__state_province),
  references (_hook__reference__country_region, _hook__territory__sales)
);

WITH staging AS (
  SELECT
    state_province_id AS state_province__state_province_id,
    state_province_code AS state_province__state_province_code,
    country_region_code AS state_province__country_region_code,
    is_only_state_province_flag AS state_province__is_only_state_province_flag,
    name AS state_province__name,
    territory_id AS state_province__territory_id,
    rowguid AS state_province__rowguid,
    modified_date AS state_province__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS state_province__record_loaded_at
  FROM das.raw__adventure_works__state_provinces
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY state_province__state_province_id ORDER BY state_province__record_loaded_at) AS state_province__record_version,
    CASE
      WHEN state_province__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE state_province__record_loaded_at
    END AS state_province__record_valid_from,
    COALESCE(
      LEAD(state_province__record_loaded_at) OVER (PARTITION BY state_province__state_province_id ORDER BY state_province__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS state_province__record_valid_to,
    state_province__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS state_province__is_current_record,
    CASE
      WHEN state_province__is_current_record
      THEN state_province__record_loaded_at
      ELSE state_province__record_valid_to
    END AS state_province__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'reference__state_province__adventure_works|',
      state_province__state_province_id,
      '~epoch|valid_from|',
      state_province__record_valid_from
    )::BLOB AS _pit_hook__reference__state_province,
    CONCAT('reference__state_province__adventure_works|', state_province__state_province_id) AS _hook__reference__state_province,
    CONCAT('reference__country_region__adventure_works|', state_province__country_region_code) AS _hook__reference__country_region,
    CONCAT('territory__sales__adventure_works|', state_province__territory_id) AS _hook__territory__sales,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__state_province::BLOB,
  _hook__reference__state_province::BLOB,
  _hook__reference__country_region::BLOB,
  _hook__territory__sales::BLOB,
  state_province__state_province_id::BIGINT,
  state_province__state_province_code::TEXT,
  state_province__country_region_code::TEXT,
  state_province__is_only_state_province_flag::BOOLEAN,
  state_province__name::TEXT,
  state_province__territory_id::BIGINT,
  state_province__rowguid::TEXT,
  state_province__modified_date::DATE,
  state_province__record_loaded_at::TIMESTAMP,
  state_province__record_updated_at::TIMESTAMP,
  state_province__record_version::TEXT,
  state_province__record_valid_from::TIMESTAMP,
  state_province__record_valid_to::TIMESTAMP,
  state_province__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND state_province__record_updated_at BETWEEN @start_ts AND @end_ts