MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__address, _pit_hook__reference__country_region, _pit_hook__reference__state_province, _pit_hook__territory__sales)
);

WITH cte__bridge AS (
  SELECT
    'addresses' AS peripheral,
    _pit_hook__address,
    _hook__address,
    _hook__reference__state_province,
    address__record_loaded_at AS bridge__record_loaded_at,
    address__record_updated_at AS bridge__record_updated_at,
    address__record_valid_from AS bridge__record_valid_from,
    address__record_valid_to AS bridge__record_valid_to,
    address__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__addresses
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__address,
    uss_bridge__state_provinces._pit_hook__reference__state_province,
    uss_bridge__state_provinces._pit_hook__territory__sales,
    uss_bridge__state_provinces._pit_hook__reference__country_region,
    cte__bridge._hook__address,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__state_provinces.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__state_provinces.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__state_provinces.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__state_provinces.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record::BOOL,
          uss_bridge__state_provinces.bridge__is_current_record::BOOL
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN silver.uss_bridge__state_provinces
  ON cte__bridge._hook__reference__state_province = uss_bridge__state_provinces._hook__reference__state_province
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__state_provinces.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__state_provinces.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__address,
      _pit_hook__reference__country_region,
      _pit_hook__reference__state_province,
      _pit_hook__territory__sales
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__address::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__reference__state_province::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__address::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts