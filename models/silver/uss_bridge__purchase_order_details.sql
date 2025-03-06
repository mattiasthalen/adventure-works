MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__department, _pit_hook__order__purchase, _pit_hook__order_line__purchase, _pit_hook__person__employee, _pit_hook__product, _pit_hook__product_category, _pit_hook__product_subcategory, _pit_hook__reference__product_model, _pit_hook__reference__shift, _pit_hook__reference__unit_measure, _pit_hook__ship_method, _pit_hook__vendor)
);

WITH cte__bridge AS (
  SELECT
    'purchase_order_details' AS peripheral,
    _pit_hook__order_line__purchase,
    _hook__order_line__purchase,
    _hook__order__purchase,
    _hook__product,
    _hook__product,
    _hook__product,
    purchase_order_detail__record_loaded_at AS bridge__record_loaded_at,
    purchase_order_detail__record_updated_at AS bridge__record_updated_at,
    purchase_order_detail__record_valid_from AS bridge__record_valid_from,
    purchase_order_detail__record_valid_to AS bridge__record_valid_to,
    purchase_order_detail__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__purchase_order_details
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__order_line__purchase,
    uss_bridge__purchase_order_headers._pit_hook__order__purchase,
    uss_bridge__purchase_order_headers._pit_hook__reference__unit_measure,
    uss_bridge__purchase_order_headers._pit_hook__person__employee,
    uss_bridge__purchase_order_headers._pit_hook__ship_method,
    uss_bridge__purchase_order_headers._pit_hook__product_subcategory,
    uss_bridge__purchase_order_headers._pit_hook__vendor,
    uss_bridge__purchase_order_headers._pit_hook__reference__shift,
    uss_bridge__purchase_order_headers._pit_hook__product,
    uss_bridge__purchase_order_headers._pit_hook__reference__product_model,
    uss_bridge__purchase_order_headers._pit_hook__department,
    uss_bridge__purchase_order_headers._pit_hook__product_category,
    uss_bridge__products._pit_hook__product,
    uss_bridge__products._pit_hook__reference__product_model,
    uss_bridge__products._pit_hook__product_category,
    uss_bridge__products._pit_hook__product_subcategory,
    uss_bridge__product_cost_histories._pit_hook__product,
    uss_bridge__product_list_price_histories._pit_hook__product,
    cte__bridge._hook__order_line__purchase,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__purchase_order_headers.bridge__record_loaded_at,
        uss_bridge__products.bridge__record_loaded_at,
        uss_bridge__product_cost_histories.bridge__record_loaded_at,
        uss_bridge__product_list_price_histories.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__purchase_order_headers.bridge__record_updated_at,
        uss_bridge__products.bridge__record_updated_at,
        uss_bridge__product_cost_histories.bridge__record_updated_at,
        uss_bridge__product_list_price_histories.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__purchase_order_headers.bridge__record_valid_from,
        uss_bridge__products.bridge__record_valid_from,
        uss_bridge__product_cost_histories.bridge__record_valid_from,
        uss_bridge__product_list_price_histories.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__purchase_order_headers.bridge__record_valid_to,
        uss_bridge__products.bridge__record_valid_to,
        uss_bridge__product_cost_histories.bridge__record_valid_to,
        uss_bridge__product_list_price_histories.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record::BOOL,
          uss_bridge__purchase_order_headers.bridge__is_current_record::BOOL,
          uss_bridge__products.bridge__is_current_record::BOOL,
          uss_bridge__product_cost_histories.bridge__is_current_record::BOOL,
          uss_bridge__product_list_price_histories.bridge__is_current_record::BOOL
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN silver.uss_bridge__purchase_order_headers
  ON cte__bridge._hook__order__purchase = uss_bridge__purchase_order_headers._hook__order__purchase
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__purchase_order_headers.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__purchase_order_headers.bridge__record_valid_to
  LEFT JOIN silver.uss_bridge__products
  ON cte__bridge._hook__product = uss_bridge__products._hook__product
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__products.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__products.bridge__record_valid_to
  LEFT JOIN silver.uss_bridge__product_cost_histories
  ON cte__bridge._hook__product = uss_bridge__product_cost_histories._hook__product
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__product_cost_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__product_cost_histories.bridge__record_valid_to
  LEFT JOIN silver.uss_bridge__product_list_price_histories
  ON cte__bridge._hook__product = uss_bridge__product_list_price_histories._hook__product
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__product_list_price_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__product_list_price_histories.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__department,
      _pit_hook__order__purchase,
      _pit_hook__order_line__purchase,
      _pit_hook__person__employee,
      _pit_hook__product,
      _pit_hook__product_category,
      _pit_hook__product_subcategory,
      _pit_hook__reference__product_model,
      _pit_hook__reference__shift,
      _pit_hook__reference__unit_measure,
      _pit_hook__ship_method,
      _pit_hook__vendor
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__department::BLOB,
  _pit_hook__order__purchase::BLOB,
  _pit_hook__order_line__purchase::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__shift::BLOB,
  _pit_hook__reference__unit_measure::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__vendor::BLOB,
  _hook__order_line__purchase::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts