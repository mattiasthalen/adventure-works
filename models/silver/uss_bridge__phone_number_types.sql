MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__phone_number_type)
);

WITH cte__bridge AS (
  SELECT
    'phone_number_types' AS peripheral,
    _pit_hook__reference__phone_number_type,
    _hook__reference__phone_number_type,
    phone_number_type__record_loaded_at AS bridge__record_loaded_at,
    phone_number_type__record_updated_at AS bridge__record_updated_at,
    phone_number_type__record_valid_from AS bridge__record_valid_from,
    phone_number_type__record_valid_to AS bridge__record_valid_to,
    phone_number_type__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__phone_number_types
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__reference__phone_number_type
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__phone_number_type::BLOB,
  _hook__reference__phone_number_type::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts