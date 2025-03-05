MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__credit_card)
);

SELECT
  'credit_cards' AS peripheral,
  _hook__credit_card::BLOB,
  credit_card__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  credit_card__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  credit_card__record_version::TEXT AS bridge__record_version,
  credit_card__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  credit_card__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  credit_card__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__credit_cards
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts