MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__culture),
  description 'Bridge viewpoint of culture reference data: Lookup table containing the languages in which some AdventureWorks data is stored.',
  column_descriptions (
    _pit_hook__reference__culture = 'Point-in-time hook for culture reference',
    _hook__reference__culture = 'Primary hook to culture reference',
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
    'cultures' AS peripheral,
    _pit_hook__reference__culture,
    _hook__reference__culture,
    culture__record_loaded_at AS bridge__record_loaded_at,
    culture__record_updated_at AS bridge__record_updated_at,
    culture__record_valid_from AS bridge__record_valid_from,
    culture__record_valid_to AS bridge__record_valid_to,
    culture__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__cultures
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__reference__culture::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__culture::BLOB,
  _hook__reference__culture::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts