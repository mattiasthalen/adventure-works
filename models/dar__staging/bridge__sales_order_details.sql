MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__credit_card, _pit_hook__currency, _pit_hook__customer, _pit_hook__order__sales, _pit_hook__order_line__sales, _pit_hook__person__sales, _pit_hook__product, _pit_hook__product_category, _pit_hook__product_subcategory, _pit_hook__reference__country_region, _pit_hook__reference__product_model, _pit_hook__reference__special_offer, _pit_hook__ship_method, _pit_hook__store, _pit_hook__territory__sales),
  description 'Bridge viewpoint of sales order_line data: Individual products associated with a specific sales order. See SalesOrderHeader.',
  column_descriptions (
    _pit_hook__credit_card = 'Point-in-time hook for credit_card',
    _pit_hook__currency = 'Point-in-time hook for currency',
    _pit_hook__customer = 'Point-in-time hook for customer',
    _pit_hook__order__sales = 'Point-in-time hook for sales order',
    _pit_hook__order_line__sales = 'Point-in-time hook for sales order_line',
    _pit_hook__person__sales = 'Point-in-time hook for sales person',
    _pit_hook__product = 'Point-in-time hook for product',
    _pit_hook__product_category = 'Point-in-time hook for product_category',
    _pit_hook__product_subcategory = 'Point-in-time hook for product_subcategory',
    _pit_hook__reference__country_region = 'Point-in-time hook for country_region reference',
    _pit_hook__reference__product_model = 'Point-in-time hook for product_model reference',
    _pit_hook__reference__special_offer = 'Point-in-time hook for special_offer reference',
    _pit_hook__ship_method = 'Point-in-time hook for ship_method',
    _pit_hook__store = 'Point-in-time hook for store',
    _pit_hook__territory__sales = 'Point-in-time hook for sales territory',
    _hook__order_line__sales = 'Primary hook to sales order_line',
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
    'sales_order_details' AS peripheral,
    _pit_hook__order_line__sales,
    _hook__order_line__sales,
    _hook__order__sales,
    _hook__product,
    _hook__reference__special_offer,
    sales_order_detail__record_loaded_at AS bridge__record_loaded_at,
    sales_order_detail__record_updated_at AS bridge__record_updated_at,
    sales_order_detail__record_valid_from AS bridge__record_valid_from,
    sales_order_detail__record_valid_to AS bridge__record_valid_to,
    sales_order_detail__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__sales_order_details
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__order_line__sales,
    bridge__products._pit_hook__product,
    bridge__products._pit_hook__product_category,
    bridge__products._pit_hook__product_subcategory,
    bridge__products._pit_hook__reference__product_model,
    bridge__sales_order_headers._pit_hook__credit_card,
    bridge__sales_order_headers._pit_hook__currency,
    bridge__sales_order_headers._pit_hook__customer,
    bridge__sales_order_headers._pit_hook__order__sales,
    bridge__sales_order_headers._pit_hook__person__sales,
    bridge__sales_order_headers._pit_hook__reference__country_region,
    bridge__sales_order_headers._pit_hook__ship_method,
    bridge__sales_order_headers._pit_hook__store,
    bridge__sales_order_headers._pit_hook__territory__sales,
    bridge__special_offers._pit_hook__reference__special_offer,
    cte__bridge._hook__order_line__sales,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__sales_order_headers.bridge__record_loaded_at,
        bridge__products.bridge__record_loaded_at,
        bridge__special_offers.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__sales_order_headers.bridge__record_updated_at,
        bridge__products.bridge__record_updated_at,
        bridge__special_offers.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__sales_order_headers.bridge__record_valid_from,
        bridge__products.bridge__record_valid_from,
        bridge__special_offers.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__sales_order_headers.bridge__record_valid_to,
        bridge__products.bridge__record_valid_to,
        bridge__special_offers.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__sales_order_headers.bridge__is_current_record,
          bridge__products.bridge__is_current_record,
          bridge__special_offers.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__sales_order_headers
  ON cte__bridge._hook__order__sales = bridge__sales_order_headers._hook__order__sales
  AND cte__bridge.bridge__record_valid_from >= bridge__sales_order_headers.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__sales_order_headers.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__products
  ON cte__bridge._hook__product = bridge__products._hook__product
  AND cte__bridge.bridge__record_valid_from >= bridge__products.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__products.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__special_offers
  ON cte__bridge._hook__reference__special_offer = bridge__special_offers._hook__reference__special_offer
  AND cte__bridge.bridge__record_valid_from >= bridge__special_offers.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__special_offers.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__credit_card::TEXT,
      _pit_hook__currency::TEXT,
      _pit_hook__customer::TEXT,
      _pit_hook__order__sales::TEXT,
      _pit_hook__order_line__sales::TEXT,
      _pit_hook__person__sales::TEXT,
      _pit_hook__product::TEXT,
      _pit_hook__product_category::TEXT,
      _pit_hook__product_subcategory::TEXT,
      _pit_hook__reference__country_region::TEXT,
      _pit_hook__reference__product_model::TEXT,
      _pit_hook__reference__special_offer::TEXT,
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
  _pit_hook__order_line__sales::BLOB,
  _pit_hook__person__sales::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__special_offer::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__store::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__order_line__sales::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts