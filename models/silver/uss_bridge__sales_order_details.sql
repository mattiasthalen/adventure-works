MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__order_line__sales),
  references (_hook__order__sales, _hook__product, _hook__reference__special_offer)
);

SELECT
  'sales_order_details' AS peripheral,
  _hook__order_line__sales::BLOB,
  _hook__order__sales::BLOB,
  _hook__product::BLOB,
  _hook__reference__special_offer::BLOB,
  sales_order_detail__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  sales_order_detail__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  sales_order_detail__record_version::TEXT AS bridge__record_version,
  sales_order_detail__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  sales_order_detail__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  sales_order_detail__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__sales_order_details
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts