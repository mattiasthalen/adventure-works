MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__reference__address_type)
);

SELECT
  'address_types' AS peripheral,
  _hook__reference__address_type::BLOB,
  address_type__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  address_type__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  address_type__record_version::TEXT AS bridge__record_version,
  address_type__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  address_type__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  address_type__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__address_types
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts