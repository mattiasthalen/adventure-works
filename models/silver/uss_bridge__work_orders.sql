MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__order__work),
  references (_hook__product, _hook__reference__scrap_reason)
);

SELECT
  'work_orders' AS peripheral,
  _hook__order__work::BLOB,
  _hook__product::BLOB,
  _hook__reference__scrap_reason::BLOB,
  work_order__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  work_order__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  work_order__record_version::TEXT AS bridge__record_version,
  work_order__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  work_order__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  work_order__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__work_orders
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts