MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__country_region, _pit_hook__reference__state_province, _pit_hook__territory__sales)
);

WITH cte__bridge AS (
  SELECT
    'state_provinces' AS peripheral,
    _pit_hook__reference__state_province,
    _hook__reference__state_province,
    _hook__reference__country_region,
    _hook__territory__sales,
    state_province__record_loaded_at AS bridge__record_loaded_at,
    state_province__record_updated_at AS bridge__record_updated_at,
    state_province__record_valid_from AS bridge__record_valid_from,
    state_province__record_valid_to AS bridge__record_valid_to,
    state_province__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__state_provinces
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__reference__state_province,
    bridge__country_regions._pit_hook__reference__country_region,
    bridge__sales_territories._pit_hook__reference__country_region,
    bridge__sales_territories._pit_hook__territory__sales,
    cte__bridge._hook__reference__state_province,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__country_regions.bridge__record_loaded_at,
        bridge__sales_territories.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__country_regions.bridge__record_updated_at,
        bridge__sales_territories.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__country_regions.bridge__record_valid_from,
        bridge__sales_territories.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__country_regions.bridge__record_valid_to,
        bridge__sales_territories.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__country_regions.bridge__is_current_record,
          bridge__sales_territories.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__country_regions
  ON cte__bridge._hook__reference__country_region = bridge__country_regions._hook__reference__country_region
  AND cte__bridge.bridge__record_valid_from >= bridge__country_regions.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__country_regions.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__sales_territories
  ON cte__bridge._hook__territory__sales = bridge__sales_territories._hook__territory__sales
  AND cte__bridge.bridge__record_valid_from >= bridge__sales_territories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__sales_territories.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__reference__country_region::TEXT,
      _pit_hook__reference__state_province::TEXT,
      _pit_hook__territory__sales::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__reference__state_province::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__reference__state_province::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts