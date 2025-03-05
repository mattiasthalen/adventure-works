MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__ship_method)
);

SELECT
  'ship_methods' AS peripheral,
  _hook__ship_method::BLOB,
  ship_method__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  ship_method__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  ship_method__record_version::TEXT AS bridge__record_version,
  ship_method__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  ship_method__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  ship_method__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__ship_methods
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts