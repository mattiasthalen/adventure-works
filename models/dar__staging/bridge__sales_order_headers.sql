MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__credit_card, _pit_hook__currency, _pit_hook__customer, _pit_hook__order__sales, _pit_hook__person__sales, _pit_hook__reference__country_region, _pit_hook__ship_method, _pit_hook__store, _pit_hook__territory__sales)
);

WITH cte__bridge AS (
  SELECT
    'sales_order_headers' AS peripheral,
    _pit_hook__order__sales,
    _hook__order__sales,
    _hook__customer,
    _hook__person__sales,
    _hook__person__sales,
    _hook__person__sales,
    _hook__territory__sales,
    _hook__ship_method,
    _hook__credit_card,
    _hook__currency,
    sales_order_header__record_loaded_at AS bridge__record_loaded_at,
    sales_order_header__record_updated_at AS bridge__record_updated_at,
    sales_order_header__record_valid_from AS bridge__record_valid_from,
    sales_order_header__record_valid_to AS bridge__record_valid_to,
    sales_order_header__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__sales_order_headers
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__order__sales,
    bridge__credit_cards._pit_hook__credit_card,
    bridge__currencies._pit_hook__currency,
    bridge__customers._pit_hook__customer,
    bridge__customers._pit_hook__person__sales,
    bridge__customers._pit_hook__reference__country_region,
    bridge__customers._pit_hook__store,
    bridge__customers._pit_hook__territory__sales,
    bridge__sales_person_quota_histories._pit_hook__person__sales,
    bridge__sales_persons._pit_hook__person__sales,
    bridge__sales_persons._pit_hook__reference__country_region,
    bridge__sales_persons._pit_hook__territory__sales,
    bridge__sales_territories._pit_hook__reference__country_region,
    bridge__sales_territories._pit_hook__territory__sales,
    bridge__sales_territory_histories._pit_hook__person__sales,
    bridge__sales_territory_histories._pit_hook__reference__country_region,
    bridge__sales_territory_histories._pit_hook__territory__sales,
    bridge__ship_methods._pit_hook__ship_method,
    cte__bridge._hook__order__sales,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__customers.bridge__record_loaded_at,
        bridge__sales_persons.bridge__record_loaded_at,
        bridge__sales_territory_histories.bridge__record_loaded_at,
        bridge__sales_person_quota_histories.bridge__record_loaded_at,
        bridge__sales_territories.bridge__record_loaded_at,
        bridge__ship_methods.bridge__record_loaded_at,
        bridge__credit_cards.bridge__record_loaded_at,
        bridge__currencies.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__customers.bridge__record_updated_at,
        bridge__sales_persons.bridge__record_updated_at,
        bridge__sales_territory_histories.bridge__record_updated_at,
        bridge__sales_person_quota_histories.bridge__record_updated_at,
        bridge__sales_territories.bridge__record_updated_at,
        bridge__ship_methods.bridge__record_updated_at,
        bridge__credit_cards.bridge__record_updated_at,
        bridge__currencies.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__customers.bridge__record_valid_from,
        bridge__sales_persons.bridge__record_valid_from,
        bridge__sales_territory_histories.bridge__record_valid_from,
        bridge__sales_person_quota_histories.bridge__record_valid_from,
        bridge__sales_territories.bridge__record_valid_from,
        bridge__ship_methods.bridge__record_valid_from,
        bridge__credit_cards.bridge__record_valid_from,
        bridge__currencies.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__customers.bridge__record_valid_to,
        bridge__sales_persons.bridge__record_valid_to,
        bridge__sales_territory_histories.bridge__record_valid_to,
        bridge__sales_person_quota_histories.bridge__record_valid_to,
        bridge__sales_territories.bridge__record_valid_to,
        bridge__ship_methods.bridge__record_valid_to,
        bridge__credit_cards.bridge__record_valid_to,
        bridge__currencies.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__customers.bridge__is_current_record,
          bridge__sales_persons.bridge__is_current_record,
          bridge__sales_territory_histories.bridge__is_current_record,
          bridge__sales_person_quota_histories.bridge__is_current_record,
          bridge__sales_territories.bridge__is_current_record,
          bridge__ship_methods.bridge__is_current_record,
          bridge__credit_cards.bridge__is_current_record,
          bridge__currencies.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__customers
  ON cte__bridge._hook__customer = bridge__customers._hook__customer
  AND cte__bridge.bridge__record_valid_from >= bridge__customers.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__customers.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__sales_persons
  ON cte__bridge._hook__person__sales = bridge__sales_persons._hook__person__sales
  AND cte__bridge.bridge__record_valid_from >= bridge__sales_persons.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__sales_persons.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__sales_territory_histories
  ON cte__bridge._hook__person__sales = bridge__sales_territory_histories._hook__person__sales
  AND cte__bridge.bridge__record_valid_from >= bridge__sales_territory_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__sales_territory_histories.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__sales_person_quota_histories
  ON cte__bridge._hook__person__sales = bridge__sales_person_quota_histories._hook__person__sales
  AND cte__bridge.bridge__record_valid_from >= bridge__sales_person_quota_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__sales_person_quota_histories.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__sales_territories
  ON cte__bridge._hook__territory__sales = bridge__sales_territories._hook__territory__sales
  AND cte__bridge.bridge__record_valid_from >= bridge__sales_territories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__sales_territories.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__ship_methods
  ON cte__bridge._hook__ship_method = bridge__ship_methods._hook__ship_method
  AND cte__bridge.bridge__record_valid_from >= bridge__ship_methods.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__ship_methods.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__credit_cards
  ON cte__bridge._hook__credit_card = bridge__credit_cards._hook__credit_card
  AND cte__bridge.bridge__record_valid_from >= bridge__credit_cards.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__credit_cards.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__currencies
  ON cte__bridge._hook__currency = bridge__currencies._hook__currency
  AND cte__bridge.bridge__record_valid_from >= bridge__currencies.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__currencies.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__credit_card::TEXT,
      _pit_hook__currency::TEXT,
      _pit_hook__customer::TEXT,
      _pit_hook__order__sales::TEXT,
      _pit_hook__person__sales::TEXT,
      _pit_hook__reference__country_region::TEXT,
      _pit_hook__ship_method::TEXT,
      _pit_hook__store::TEXT,
      _pit_hook__territory__sales::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__credit_card::BLOB,
  _pit_hook__currency::BLOB,
  _pit_hook__customer::BLOB,
  _pit_hook__order__sales::BLOB,
  _pit_hook__person__sales::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__store::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__order__sales::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts