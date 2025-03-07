MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__country_region, _pit_hook__territory__sales)
);

WITH cte__bridge AS (
  SELECT
    'sales_territories' AS peripheral,
    _pit_hook__territory__sales,
    _hook__territory__sales,
    _hook__reference__country_region,
    sales_territory__record_loaded_at AS bridge__record_loaded_at,
    sales_territory__record_updated_at AS bridge__record_updated_at,
    sales_territory__record_valid_from AS bridge__record_valid_from,
    sales_territory__record_valid_to AS bridge__record_valid_to,
    sales_territory__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__sales_territories
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__territory__sales,
    uss_bridge__country_regions._pit_hook__reference__country_region,
    cte__bridge._hook__territory__sales,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__country_regions.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__country_regions.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__country_regions.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__country_regions.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record::BOOL,
          uss_bridge__country_regions.bridge__is_current_record::BOOL
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN silver.uss_bridge__country_regions
  ON cte__bridge._hook__reference__country_region = uss_bridge__country_regions._hook__reference__country_region
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__country_regions.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__country_regions.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__reference__country_region,
      _pit_hook__territory__sales
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__territory__sales::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts