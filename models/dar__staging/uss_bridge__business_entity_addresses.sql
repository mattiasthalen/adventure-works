MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__address, _pit_hook__business_entity, _pit_hook__reference__address_type, _pit_hook__reference__contact_type)
);

WITH cte__bridge AS (
  SELECT
    'business_entity_addresses' AS peripheral,
    _pit_hook__address,
    _hook__address,
    _hook__business_entity,
    _hook__reference__address_type,
    _hook__epoch__date,
    measure__business_entity_addresses_modified,
    business_entity_address__record_loaded_at AS bridge__record_loaded_at,
    business_entity_address__record_updated_at AS bridge__record_updated_at,
    business_entity_address__record_valid_from AS bridge__record_valid_from,
    business_entity_address__record_valid_to AS bridge__record_valid_to,
    business_entity_address__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__business_entity_addresses
  LEFT JOIN dar__staging.measure__adventure_works__business_entity_addresses USING (_pit_hook__address)
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__address,
    uss_bridge__address_types._pit_hook__reference__address_type,
    uss_bridge__business_entity_contacts._pit_hook__business_entity,
    uss_bridge__business_entity_contacts._pit_hook__reference__contact_type,
    cte__bridge._hook__address,
    cte__bridge._hook__epoch__date,
    cte__bridge.measure__business_entity_addresses_modified,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__business_entity_contacts.bridge__record_loaded_at,
        uss_bridge__address_types.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__business_entity_contacts.bridge__record_updated_at,
        uss_bridge__address_types.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__business_entity_contacts.bridge__record_valid_from,
        uss_bridge__address_types.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__business_entity_contacts.bridge__record_valid_to,
        uss_bridge__address_types.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          uss_bridge__business_entity_contacts.bridge__is_current_record,
          uss_bridge__address_types.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.uss_bridge__business_entity_contacts
  ON cte__bridge._hook__business_entity = uss_bridge__business_entity_contacts._hook__business_entity
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__business_entity_contacts.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__business_entity_contacts.bridge__record_valid_to
  LEFT JOIN dar__staging.uss_bridge__address_types
  ON cte__bridge._hook__reference__address_type = uss_bridge__address_types._hook__reference__address_type
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__address_types.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__address_types.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _hook__epoch__date::TEXT,
      _pit_hook__address::TEXT,
      _pit_hook__business_entity::TEXT,
      _pit_hook__reference__address_type::TEXT,
      _pit_hook__reference__contact_type::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__address::BLOB,
  _pit_hook__business_entity::BLOB,
  _pit_hook__reference__address_type::BLOB,
  _pit_hook__reference__contact_type::BLOB,
  _hook__address::BLOB,
  _hook__epoch__date::BLOB,
  measure__business_entity_addresses_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts