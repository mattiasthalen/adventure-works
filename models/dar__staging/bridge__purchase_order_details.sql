MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__order__purchase, _pit_hook__order_line__purchase, _pit_hook__person__employee, _pit_hook__product, _pit_hook__product_category, _pit_hook__product_subcategory, _pit_hook__reference__product_model, _pit_hook__ship_method, _pit_hook__vendor)
);

WITH cte__bridge AS (
  SELECT
    'purchase_order_details' AS peripheral,
    _pit_hook__order_line__purchase,
    _hook__order_line__purchase,
    _hook__order__purchase,
    _hook__product,
    purchase_order_detail__record_loaded_at AS bridge__record_loaded_at,
    purchase_order_detail__record_updated_at AS bridge__record_updated_at,
    purchase_order_detail__record_valid_from AS bridge__record_valid_from,
    purchase_order_detail__record_valid_to AS bridge__record_valid_to,
    purchase_order_detail__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__purchase_order_details
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__order_line__purchase,
    bridge__products._pit_hook__product,
    bridge__products._pit_hook__product_category,
    bridge__products._pit_hook__product_subcategory,
    bridge__products._pit_hook__reference__product_model,
    bridge__purchase_order_headers._pit_hook__order__purchase,
    bridge__purchase_order_headers._pit_hook__person__employee,
    bridge__purchase_order_headers._pit_hook__ship_method,
    bridge__purchase_order_headers._pit_hook__vendor,
    cte__bridge._hook__order_line__purchase,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__purchase_order_headers.bridge__record_loaded_at,
        bridge__products.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__purchase_order_headers.bridge__record_updated_at,
        bridge__products.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__purchase_order_headers.bridge__record_valid_from,
        bridge__products.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__purchase_order_headers.bridge__record_valid_to,
        bridge__products.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__purchase_order_headers.bridge__is_current_record,
          bridge__products.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__purchase_order_headers
  ON cte__bridge._hook__order__purchase = bridge__purchase_order_headers._hook__order__purchase
  AND cte__bridge.bridge__record_valid_from >= bridge__purchase_order_headers.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__purchase_order_headers.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__products
  ON cte__bridge._hook__product = bridge__products._hook__product
  AND cte__bridge.bridge__record_valid_from >= bridge__products.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__products.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__order__purchase::TEXT,
      _pit_hook__order_line__purchase::TEXT,
      _pit_hook__person__employee::TEXT,
      _pit_hook__product::TEXT,
      _pit_hook__product_category::TEXT,
      _pit_hook__product_subcategory::TEXT,
      _pit_hook__reference__product_model::TEXT,
      _pit_hook__ship_method::TEXT,
      _pit_hook__vendor::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__order__purchase::BLOB,
  _pit_hook__order_line__purchase::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__product_model::BLOB,
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