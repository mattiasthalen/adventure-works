MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__address),
  references (_hook__business_entity, _hook__reference__address_type)
);

SELECT
  'business_entity_addresses' AS peripheral,
  _hook__address::BLOB,
  _hook__business_entity::BLOB,
  _hook__reference__address_type::BLOB,
  business_entity_address__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  business_entity_address__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  business_entity_address__record_version::TEXT AS bridge__record_version,
  business_entity_address__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  business_entity_address__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  business_entity_address__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__business_entity_addresses
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts