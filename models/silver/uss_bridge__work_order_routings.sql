MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__order_line__work),
  references (_hook__order__work, _hook__product, _hook__reference__location)
);

SELECT
  'work_order_routings' AS peripheral,
  _hook__order_line__work::BLOB,
  _hook__order__work::BLOB,
  _hook__product::BLOB,
  _hook__reference__location::BLOB,
  work_order_routing__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  work_order_routing__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  work_order_routing__record_version::TEXT AS bridge__record_version,
  work_order_routing__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  work_order_routing__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  work_order_routing__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__work_order_routings
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts