MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__business_entity, _pit_hook__reference__contact_type)
);

WITH cte__bridge AS (
  SELECT
    'business_entity_contacts' AS peripheral,
    _pit_hook__business_entity,
    _hook__business_entity,
    _hook__reference__contact_type,
    business_entity_contact__record_loaded_at AS bridge__record_loaded_at,
    business_entity_contact__record_updated_at AS bridge__record_updated_at,
    business_entity_contact__record_valid_from AS bridge__record_valid_from,
    business_entity_contact__record_valid_to AS bridge__record_valid_to,
    business_entity_contact__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__business_entity_contacts
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__business_entity,
    bridge__contact_types._pit_hook__reference__contact_type,
    cte__bridge._hook__business_entity,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__contact_types.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__contact_types.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__contact_types.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__contact_types.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__contact_types.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__contact_types
  ON cte__bridge._hook__reference__contact_type = bridge__contact_types._hook__reference__contact_type
  AND cte__bridge.bridge__record_valid_from >= bridge__contact_types.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__contact_types.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__business_entity::TEXT,
      _pit_hook__reference__contact_type::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__business_entity::BLOB,
  _pit_hook__reference__contact_type::BLOB,
  _hook__business_entity::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts