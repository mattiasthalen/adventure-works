MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__reference__state_province),
  references (_hook__reference__country_region, _hook__territory__sales)
);

SELECT
  'state_provinces' AS peripheral,
  _hook__reference__state_province::BLOB,
  _hook__reference__country_region::BLOB,
  _hook__territory__sales::BLOB,
  state_province__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  state_province__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  state_province__record_version::TEXT AS bridge__record_version,
  state_province__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  state_province__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  state_province__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__state_provinces
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts