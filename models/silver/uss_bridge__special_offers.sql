MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__reference__special_offer)
);

SELECT
  'special_offers' AS peripheral,
  _hook__reference__special_offer::BLOB,
  special_offer__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  special_offer__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  special_offer__record_version::TEXT AS bridge__record_version,
  special_offer__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  special_offer__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  special_offer__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__special_offers
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts