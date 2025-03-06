MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__currency)
);

WITH cte__bridge AS (
  SELECT
    'currencies' AS peripheral,
    _pit_hook__currency,
    _hook__currency,
    currency__record_loaded_at AS bridge__record_loaded_at,
    currency__record_updated_at AS bridge__record_updated_at,
    currency__record_valid_from AS bridge__record_valid_from,
    currency__record_valid_to AS bridge__record_valid_to,
    currency__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__currencies
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__currency
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__currency::BLOB,
  _hook__currency::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts