MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__product, _pit_hook__product_category, _pit_hook__product_subcategory, _pit_hook__reference__product_model, _pit_hook__reference__unit_measure, _pit_hook__vendor)
);

WITH cte__bridge AS (
  SELECT
    'product_vendors' AS peripheral,
    _pit_hook__vendor,
    _hook__vendor,
    _hook__product,
    _hook__product,
    _hook__product,
    _hook__reference__unit_measure,
    product_vendor__record_loaded_at AS bridge__record_loaded_at,
    product_vendor__record_updated_at AS bridge__record_updated_at,
    product_vendor__record_valid_from AS bridge__record_valid_from,
    product_vendor__record_valid_to AS bridge__record_valid_to,
    product_vendor__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__product_vendors
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__vendor,
    bridge__product_cost_histories._pit_hook__product,
    bridge__product_list_price_histories._pit_hook__product,
    bridge__products._pit_hook__product,
    bridge__products._pit_hook__product_category,
    bridge__products._pit_hook__product_subcategory,
    bridge__products._pit_hook__reference__product_model,
    bridge__unit_measures._pit_hook__reference__unit_measure,
    cte__bridge._hook__vendor,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__products.bridge__record_loaded_at,
        bridge__product_cost_histories.bridge__record_loaded_at,
        bridge__product_list_price_histories.bridge__record_loaded_at,
        bridge__unit_measures.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__products.bridge__record_updated_at,
        bridge__product_cost_histories.bridge__record_updated_at,
        bridge__product_list_price_histories.bridge__record_updated_at,
        bridge__unit_measures.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__products.bridge__record_valid_from,
        bridge__product_cost_histories.bridge__record_valid_from,
        bridge__product_list_price_histories.bridge__record_valid_from,
        bridge__unit_measures.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__products.bridge__record_valid_to,
        bridge__product_cost_histories.bridge__record_valid_to,
        bridge__product_list_price_histories.bridge__record_valid_to,
        bridge__unit_measures.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__products.bridge__is_current_record,
          bridge__product_cost_histories.bridge__is_current_record,
          bridge__product_list_price_histories.bridge__is_current_record,
          bridge__unit_measures.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__products
  ON cte__bridge._hook__product = bridge__products._hook__product
  AND cte__bridge.bridge__record_valid_from >= bridge__products.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__products.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__product_cost_histories
  ON cte__bridge._hook__product = bridge__product_cost_histories._hook__product
  AND cte__bridge.bridge__record_valid_from >= bridge__product_cost_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__product_cost_histories.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__product_list_price_histories
  ON cte__bridge._hook__product = bridge__product_list_price_histories._hook__product
  AND cte__bridge.bridge__record_valid_from >= bridge__product_list_price_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__product_list_price_histories.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__unit_measures
  ON cte__bridge._hook__reference__unit_measure = bridge__unit_measures._hook__reference__unit_measure
  AND cte__bridge.bridge__record_valid_from >= bridge__unit_measures.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__unit_measures.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__product::TEXT,
      _pit_hook__product_category::TEXT,
      _pit_hook__product_subcategory::TEXT,
      _pit_hook__reference__product_model::TEXT,
      _pit_hook__reference__unit_measure::TEXT,
      _pit_hook__vendor::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__unit_measure::BLOB,
  _pit_hook__vendor::BLOB,
  _hook__vendor::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts