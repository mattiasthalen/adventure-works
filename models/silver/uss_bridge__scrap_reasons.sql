MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__reference__scrap_reason)
);

SELECT
  'scrap_reasons' AS peripheral,
  _hook__reference__scrap_reason::BLOB,
  scrap_reason__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  scrap_reason__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  scrap_reason__record_version::TEXT AS bridge__record_version,
  scrap_reason__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  scrap_reason__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  scrap_reason__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__scrap_reasons
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts