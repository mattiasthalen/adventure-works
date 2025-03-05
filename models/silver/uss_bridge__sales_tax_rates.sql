MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__reference__sales_tax_rate),
  references (_hook__reference__state_province)
);

SELECT
  'sales_tax_rates' AS peripheral,
  _hook__reference__sales_tax_rate::BLOB,
  _hook__reference__state_province::BLOB,
  sales_tax_rate__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  sales_tax_rate__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  sales_tax_rate__record_version::TEXT AS bridge__record_version,
  sales_tax_rate__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  sales_tax_rate__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  sales_tax_rate__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__sales_tax_rates
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts