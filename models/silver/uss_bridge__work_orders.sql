MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__order__work, _pit_hook__product, _pit_hook__product_category, _pit_hook__product_subcategory, _pit_hook__reference__product_model, _pit_hook__reference__scrap_reason)
);

WITH cte__bridge AS (
  SELECT
    'work_orders' AS peripheral,
    _pit_hook__order__work,
    _hook__order__work,
    _hook__product,
    _hook__product,
    _hook__product,
    _hook__reference__scrap_reason,
    _hook__epoch__date,
    measure__work_orders_started,
    measure__work_orders_finished,
    measure__work_orders_due,
    work_order__record_loaded_at AS bridge__record_loaded_at,
    work_order__record_updated_at AS bridge__record_updated_at,
    work_order__record_valid_from AS bridge__record_valid_from,
    work_order__record_valid_to AS bridge__record_valid_to,
    work_order__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__work_orders
  LEFT JOIN silver.measure__adventure_works__work_orders USING (_pit_hook__order__work)
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__order__work,
    uss_bridge__products._pit_hook__product,
    uss_bridge__products._pit_hook__product_category,
    uss_bridge__products._pit_hook__product_subcategory,
    uss_bridge__products._pit_hook__reference__product_model,
    uss_bridge__product_cost_histories._pit_hook__product,
    uss_bridge__product_list_price_histories._pit_hook__product,
    uss_bridge__scrap_reasons._pit_hook__reference__scrap_reason,
    cte__bridge._hook__order__work,
    cte__bridge._hook__epoch__date,
    cte__bridge.measure__work_orders_started,
    cte__bridge.measure__work_orders_finished,
    cte__bridge.measure__work_orders_due,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__products.bridge__record_loaded_at,
        uss_bridge__product_cost_histories.bridge__record_loaded_at,
        uss_bridge__product_list_price_histories.bridge__record_loaded_at,
        uss_bridge__scrap_reasons.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__products.bridge__record_updated_at,
        uss_bridge__product_cost_histories.bridge__record_updated_at,
        uss_bridge__product_list_price_histories.bridge__record_updated_at,
        uss_bridge__scrap_reasons.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__products.bridge__record_valid_from,
        uss_bridge__product_cost_histories.bridge__record_valid_from,
        uss_bridge__product_list_price_histories.bridge__record_valid_from,
        uss_bridge__scrap_reasons.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__products.bridge__record_valid_to,
        uss_bridge__product_cost_histories.bridge__record_valid_to,
        uss_bridge__product_list_price_histories.bridge__record_valid_to,
        uss_bridge__scrap_reasons.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record::BOOL,
          uss_bridge__products.bridge__is_current_record::BOOL,
          uss_bridge__product_cost_histories.bridge__is_current_record::BOOL,
          uss_bridge__product_list_price_histories.bridge__is_current_record::BOOL,
          uss_bridge__scrap_reasons.bridge__is_current_record::BOOL
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
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
  LEFT JOIN silver.uss_bridge__scrap_reasons
  ON cte__bridge._hook__reference__scrap_reason = uss_bridge__scrap_reasons._hook__reference__scrap_reason
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__scrap_reasons.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__scrap_reasons.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__order__work,
      _pit_hook__product,
      _pit_hook__product_category,
      _pit_hook__product_subcategory,
      _pit_hook__reference__product_model,
      _pit_hook__reference__scrap_reason
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__order__work::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__scrap_reason::BLOB,
  _hook__order__work::BLOB,
  _hook__epoch__date::BLOB,
  measure__work_orders_started::INT,
  measure__work_orders_finished::INT,
  measure__work_orders_due::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts