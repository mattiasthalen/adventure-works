MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__vendor),
  references (_hook__product, _hook__reference__unit_measure)
);

SELECT
  'product_vendors' AS peripheral,
  _hook__vendor::BLOB,
  _hook__product::BLOB,
  _hook__reference__unit_measure::BLOB,
  product_vendor__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  product_vendor__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  product_vendor__record_version::TEXT AS bridge__record_version,
  product_vendor__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  product_vendor__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  product_vendor__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__product_vendors
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts