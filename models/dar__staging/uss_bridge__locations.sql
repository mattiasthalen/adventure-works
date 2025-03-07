MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__location)
);

WITH cte__bridge AS (
  SELECT
    'locations' AS peripheral,
    _pit_hook__reference__location,
    _hook__reference__location,
    _hook__epoch__date,
    measure__locations_modified,
    location__record_loaded_at AS bridge__record_loaded_at,
    location__record_updated_at AS bridge__record_updated_at,
    location__record_valid_from AS bridge__record_valid_from,
    location__record_valid_to AS bridge__record_valid_to,
    location__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__locations
  LEFT JOIN dar__staging.measure__adventure_works__locations USING (_pit_hook__reference__location)
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _hook__epoch__date::TEXT,
      _pit_hook__reference__location::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__location::BLOB,
  _hook__reference__location::BLOB,
  _hook__epoch__date::BLOB,
  measure__locations_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts