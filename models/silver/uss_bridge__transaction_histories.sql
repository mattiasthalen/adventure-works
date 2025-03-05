MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__transaction_history),
  references (_hook__product, _hook__order__reference)
);

SELECT
  'transaction_histories' AS peripheral,
  _hook__transaction_history::BLOB,
  _hook__product::BLOB,
  _hook__order__reference::BLOB,
  transaction_history__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  transaction_history__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  transaction_history__record_version::TEXT AS bridge__record_version,
  transaction_history__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  transaction_history__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  transaction_history__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__transaction_histories
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts