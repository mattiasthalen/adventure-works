MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__product_category, _pit_hook__product_subcategory)
);

WITH cte__bridge AS (
  SELECT
    'product_subcategories' AS peripheral,
    _pit_hook__product_subcategory,
    _hook__product_subcategory,
    _hook__product_category,
    _hook__epoch__date,
    measure__product_subcategories_modified,
    product_subcategory__record_loaded_at AS bridge__record_loaded_at,
    product_subcategory__record_updated_at AS bridge__record_updated_at,
    product_subcategory__record_valid_from AS bridge__record_valid_from,
    product_subcategory__record_valid_to AS bridge__record_valid_to,
    product_subcategory__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__product_subcategories
  LEFT JOIN dar__staging.measure__adventure_works__product_subcategories USING (_pit_hook__product_subcategory)
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__product_subcategory,
    uss_bridge__product_categories._pit_hook__product_category,
    cte__bridge._hook__product_subcategory,
    cte__bridge._hook__epoch__date,
    cte__bridge.measure__product_subcategories_modified,
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
          cte__bridge.bridge__is_current_record,
          uss_bridge__product_categories.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.uss_bridge__product_categories
  ON cte__bridge._hook__product_category = uss_bridge__product_categories._hook__product_category
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__product_categories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__product_categories.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _hook__epoch__date::TEXT,
      _pit_hook__product_category::TEXT,
      _pit_hook__product_subcategory::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _hook__product_subcategory::BLOB,
  _hook__epoch__date::BLOB,
  measure__product_subcategories_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts