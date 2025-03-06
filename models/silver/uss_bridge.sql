MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__bridge),
  references (_pit_hook__product, _pit_hook__product_subcategory, _pit_hook__product_category)
);

WITH cte__bridge_union AS (
  SELECT * FROM silver.uss_bridge__products
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_subcategories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_categories
)
SELECT
  _pit_hook__bridge::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__product_category::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_union
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts