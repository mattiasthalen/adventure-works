MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__person__individual, _pit_hook__reference__phone_number_type),
  description 'Bridge viewpoint of individual person data: Telephone number and type of a person.',
  column_descriptions (
    _pit_hook__person__individual = 'Point-in-time hook for individual person',
    _pit_hook__reference__phone_number_type = 'Point-in-time hook for phone_number_type reference',
    _hook__person__individual = 'Primary hook to individual person',
    peripheral = 'Name of the peripheral this bridge represents',
    _pit_hook__bridge = 'Unified bridge point-in-time hook that combines peripheral and validity period',
    bridge__record_loaded_at = 'Timestamp when this bridge record was loaded',
    bridge__record_updated_at = 'Timestamp when this bridge record was last updated',
    bridge__record_valid_from = 'Timestamp from which this bridge record is valid',
    bridge__record_valid_to = 'Timestamp until which this bridge record is valid',
    bridge__is_current_record = 'Flag indicating if this is the current valid version of the bridge record'
  )
);

WITH cte__bridge AS (
  SELECT
    'person_phones' AS peripheral,
    _pit_hook__person__individual,
    _hook__person__individual,
    _hook__reference__phone_number_type,
    person_phone__record_loaded_at AS bridge__record_loaded_at,
    person_phone__record_updated_at AS bridge__record_updated_at,
    person_phone__record_valid_from AS bridge__record_valid_from,
    person_phone__record_valid_to AS bridge__record_valid_to,
    person_phone__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__person_phones
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__person__individual,
    bridge__phone_number_types._pit_hook__reference__phone_number_type,
    cte__bridge._hook__person__individual,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__phone_number_types.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__phone_number_types.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__phone_number_types.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__phone_number_types.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__phone_number_types.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__phone_number_types
  ON cte__bridge._hook__reference__phone_number_type = bridge__phone_number_types._hook__reference__phone_number_type
  AND cte__bridge.bridge__record_valid_from >= bridge__phone_number_types.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__phone_number_types.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__person__individual::TEXT,
      _pit_hook__reference__phone_number_type::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__person__individual::BLOB,
  _pit_hook__reference__phone_number_type::BLOB,
  _hook__person__individual::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts