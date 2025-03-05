MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__shopping_cart_item),
  references (_hook__product)
);

SELECT
  'shopping_cart_items' AS peripheral,
  _hook__shopping_cart_item::BLOB,
  _hook__product::BLOB,
  shopping_cart_item__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  shopping_cart_item__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  shopping_cart_item__record_version::TEXT AS bridge__record_version,
  shopping_cart_item__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  shopping_cart_item__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  shopping_cart_item__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__shopping_cart_items
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts