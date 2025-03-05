MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__reference__product_description)
);

SELECT
  'product_descriptions' AS peripheral,
  _hook__reference__product_description::BLOB,
  product_description__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  product_description__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  product_description__record_version::TEXT AS bridge__record_version,
  product_description__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  product_description__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  product_description__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__product_descriptions
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts