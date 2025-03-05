MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__transaction_history_archive),
  references (_hook__product, _hook__order__reference)
);

SELECT
  'transaction_history_archives' AS peripheral,
  _hook__transaction_history_archive::BLOB,
  _hook__product::BLOB,
  _hook__order__reference::BLOB,
  transaction_history_archive__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  transaction_history_archive__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  transaction_history_archive__record_version::TEXT AS bridge__record_version,
  transaction_history_archive__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  transaction_history_archive__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  transaction_history_archive__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__transaction_history_archives
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts