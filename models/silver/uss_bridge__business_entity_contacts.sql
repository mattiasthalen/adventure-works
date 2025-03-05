MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__business_entity),
  references (_hook__person__contact, _hook__reference__contact_type)
);

SELECT
  'business_entity_contacts' AS peripheral,
  _hook__business_entity::BLOB,
  _hook__person__contact::BLOB,
  _hook__reference__contact_type::BLOB,
  business_entity_contact__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  business_entity_contact__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  business_entity_contact__record_version::TEXT AS bridge__record_version,
  business_entity_contact__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  business_entity_contact__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  business_entity_contact__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__business_entity_contacts
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts