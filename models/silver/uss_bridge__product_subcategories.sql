MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__bridge),
  references (_pit_hook__product_subcategory, _pit_hook__product_category)
);

WITH cte__bridge AS (
  SELECT
    'product_subcategories' AS peripheral,
    _pit_hook__product_subcategory,
    _hook__product_category,
    _hook__product_subcategory,
    product_subcategory__record_loaded_at AS bridge__record_loaded_at,
    product_subcategory__record_updated_at AS bridge__record_updated_at,
    product_subcategory__record_valid_from AS bridge__record_valid_from,
    product_subcategory__record_valid_to AS bridge__record_valid_to,
    product_subcategory__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__product_subcategories
), cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__product_subcategory,
    uss_bridge__product_categories._pit_hook__product_category,
    cte__bridge._hook__product_subcategory,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__product_categories.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__product_categories.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__product_categories.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__product_categories.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record::BOOL,
          uss_bridge__product_categories.bridge__is_current_record::BOOL
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN silver.uss_bridge__product_categories
  ON cte__bridge._hook__product_category = uss_bridge__product_categories._hook__product_category
  AND cte__bridge.bridge__record_valid_from > uss_bridge__product_categories.bridge__record_valid_to
  AND cte__bridge.bridge__record_valid_to < uss_bridge__product_categories.bridge__record_valid_from
), cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__product_subcategory,
      _pit_hook__product_category
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__product_category::BLOB,
  _hook__product_subcategory::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts