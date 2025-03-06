MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__customer, _pit_hook__person__sales, _pit_hook__reference__country_region, _pit_hook__store, _pit_hook__territory__sales)
);

WITH cte__bridge AS (
  SELECT
    'customers' AS peripheral,
    _pit_hook__customer,
    _hook__customer,
    _hook__store,
    _hook__territory__sales,
    customer__record_loaded_at AS bridge__record_loaded_at,
    customer__record_updated_at AS bridge__record_updated_at,
    customer__record_valid_from AS bridge__record_valid_from,
    customer__record_valid_to AS bridge__record_valid_to,
    customer__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__customers
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__customer,
    uss_bridge__stores._pit_hook__store,
    uss_bridge__stores._pit_hook__territory__sales,
    uss_bridge__stores._pit_hook__person__sales,
    uss_bridge__stores._pit_hook__reference__country_region,
    uss_bridge__sales_territories._pit_hook__territory__sales,
    uss_bridge__sales_territories._pit_hook__reference__country_region,
    cte__bridge._hook__customer,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__stores.bridge__record_loaded_at,
        uss_bridge__sales_territories.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__stores.bridge__record_updated_at,
        uss_bridge__sales_territories.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__stores.bridge__record_valid_from,
        uss_bridge__sales_territories.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__stores.bridge__record_valid_to,
        uss_bridge__sales_territories.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record::BOOL,
          uss_bridge__stores.bridge__is_current_record::BOOL,
          uss_bridge__sales_territories.bridge__is_current_record::BOOL
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN silver.uss_bridge__stores
  ON cte__bridge._hook__store = uss_bridge__stores._hook__store
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__stores.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__stores.bridge__record_valid_to
  LEFT JOIN silver.uss_bridge__sales_territories
  ON cte__bridge._hook__territory__sales = uss_bridge__sales_territories._hook__territory__sales
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__sales_territories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__sales_territories.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__customer,
      _pit_hook__person__sales,
      _pit_hook__reference__country_region,
      _pit_hook__store,
      _pit_hook__territory__sales
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__customer::BLOB,
  _pit_hook__person__sales::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__store::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__customer::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts