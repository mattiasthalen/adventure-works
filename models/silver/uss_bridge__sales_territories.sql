MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__territory__sales),
  references (_hook__reference__country_region)
);

SELECT
  'sales_territories' AS peripheral,
  _hook__territory__sales::BLOB,
  _hook__reference__country_region::BLOB,
  sales_territory__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  sales_territory__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  sales_territory__record_version::TEXT AS bridge__record_version,
  sales_territory__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  sales_territory__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  sales_territory__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__sales_territories
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts