MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__vendor),
  description 'Bridge viewpoint of vendor data: Companies from whom Adventure Works Cycles purchases parts or other goods.',
  column_descriptions (
    _pit_hook__vendor = 'Point-in-time hook for vendor',
    _hook__vendor = 'Primary hook to vendor',
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
    'vendors' AS peripheral,
    _pit_hook__vendor,
    _hook__vendor,
    vendor__record_loaded_at AS bridge__record_loaded_at,
    vendor__record_updated_at AS bridge__record_updated_at,
    vendor__record_valid_from AS bridge__record_valid_from,
    vendor__record_valid_to AS bridge__record_valid_to,
    vendor__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__vendors
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__vendor::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__vendor::BLOB,
  _hook__vendor::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts