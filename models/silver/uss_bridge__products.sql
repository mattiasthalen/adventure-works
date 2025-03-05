MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__product),
  references (_hook__product_subcategory, _hook__reference__product_model)
);

SELECT
  'products' AS peripheral,
  _hook__product::BLOB,
  _hook__product_subcategory::BLOB,
  _hook__reference__product_model::BLOB,
  product__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  product__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  product__record_version::TEXT AS bridge__record_version,
  product__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  product__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  product__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__products
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts