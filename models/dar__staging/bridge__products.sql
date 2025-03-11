MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__product, _pit_hook__product_category, _pit_hook__product_subcategory, _pit_hook__reference__product_model)
);

WITH cte__bridge AS (
  SELECT
    'products' AS peripheral,
    _pit_hook__product,
    _hook__product,
    _hook__product_subcategory,
    _hook__reference__product_model,
    product__record_loaded_at AS bridge__record_loaded_at,
    product__record_updated_at AS bridge__record_updated_at,
    product__record_valid_from AS bridge__record_valid_from,
    product__record_valid_to AS bridge__record_valid_to,
    product__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__products
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__product,
    bridge__product_models._pit_hook__reference__product_model,
    bridge__product_subcategories._pit_hook__product_category,
    bridge__product_subcategories._pit_hook__product_subcategory,
    cte__bridge._hook__product,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__product_subcategories.bridge__record_loaded_at,
        bridge__product_models.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__product_subcategories.bridge__record_updated_at,
        bridge__product_models.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__product_subcategories.bridge__record_valid_from,
        bridge__product_models.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__product_subcategories.bridge__record_valid_to,
        bridge__product_models.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__product_subcategories.bridge__is_current_record,
          bridge__product_models.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__product_subcategories
  ON cte__bridge._hook__product_subcategory = bridge__product_subcategories._hook__product_subcategory
  AND cte__bridge.bridge__record_valid_from >= bridge__product_subcategories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__product_subcategories.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__product_models
  ON cte__bridge._hook__reference__product_model = bridge__product_models._hook__reference__product_model
  AND cte__bridge.bridge__record_valid_from >= bridge__product_models.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__product_models.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__product::TEXT,
      _pit_hook__product_category::TEXT,
      _pit_hook__product_subcategory::TEXT,
      _pit_hook__reference__product_model::TEXT
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
  _hook__product::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts