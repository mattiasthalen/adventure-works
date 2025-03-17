MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__unit_measure),
  description 'Bridge viewpoint of unit_measure reference data: Unit of measure lookup table.',
  column_descriptions (
    _pit_hook__reference__unit_measure = 'Point-in-time hook for unit_measure reference',
    _hook__reference__unit_measure = 'Primary hook to unit_measure reference',
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
    'unit_measures' AS peripheral,
    _pit_hook__reference__unit_measure,
    _hook__reference__unit_measure,
    unit_measure__record_loaded_at AS bridge__record_loaded_at,
    unit_measure__record_updated_at AS bridge__record_updated_at,
    unit_measure__record_valid_from AS bridge__record_valid_from,
    unit_measure__record_valid_to AS bridge__record_valid_to,
    unit_measure__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__unit_measures
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__reference__unit_measure::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__unit_measure::BLOB,
  _hook__reference__unit_measure::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts