MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    _pit_hook__address,
    _pit_hook__business_entity,
    _pit_hook__reference__address_type,
    _pit_hook__reference__contact_type,
    _hook__epoch__date
  )
);

WITH cte__bridge AS (
  SELECT
    peripheral,
    _pit_hook__bridge,
    _pit_hook__address,
    _pit_hook__business_entity,
    _pit_hook__reference__address_type,
    _pit_hook__reference__contact_type,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__business_entity_addresses
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__address,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'business_entity_address__modified_date' THEN 1 END) AS event__business_entity_addresses_modified
  FROM dab.bag__adventure_works__business_entity_addresses
  UNPIVOT (
    event_date FOR event IN (
      business_entity_address__modified_date
    )
  ) AS pivot__events
  GROUP BY ALL
  ORDER BY _hook__epoch__date
),
final AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      _pit_hook__bridge::TEXT,
      _hook__epoch__date::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
  LEFT JOIN cte__events USING(_pit_hook__address)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__address::BLOB,
  _pit_hook__business_entity::BLOB,
  _pit_hook__reference__address_type::BLOB,
  _pit_hook__reference__contact_type::BLOB,
  _hook__epoch__date::BLOB,
  event__business_entity_addresses_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts