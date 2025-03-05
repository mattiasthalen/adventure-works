MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__product_subcategory),
  references (_hook__product_category)
);

SELECT
  'product_subcategories' AS peripheral,
  _hook__product_subcategory::BLOB,
  _hook__product_category::BLOB,
  product_subcategory__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  product_subcategory__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  product_subcategory__record_version::TEXT AS bridge__record_version,
  product_subcategory__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  product_subcategory__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  product_subcategory__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__product_subcategories
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts