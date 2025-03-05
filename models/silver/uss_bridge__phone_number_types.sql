MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__reference__phone_number_type)
);

SELECT
  'phone_number_types' AS peripheral,
  _hook__reference__phone_number_type::BLOB,
  phone_number_type__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  phone_number_type__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  phone_number_type__record_version::TEXT AS bridge__record_version,
  phone_number_type__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  phone_number_type__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  phone_number_type__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__phone_number_types
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts