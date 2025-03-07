MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__special_offer)
);

WITH cte__bridge AS (
  SELECT
    'special_offers' AS peripheral,
    _pit_hook__reference__special_offer,
    _hook__reference__special_offer,
    _hook__epoch__date,
    measure__special_offers_started,
    measure__special_offers_finished,
    special_offer__record_loaded_at AS bridge__record_loaded_at,
    special_offer__record_updated_at AS bridge__record_updated_at,
    special_offer__record_valid_from AS bridge__record_valid_from,
    special_offer__record_valid_to AS bridge__record_valid_to,
    special_offer__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__special_offers
  LEFT JOIN silver.measure__adventure_works__special_offers USING (_pit_hook__reference__special_offer)
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__reference__special_offer
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__special_offer::BLOB,
  _hook__reference__special_offer::BLOB,
  _hook__epoch__date::BLOB,
  measure__special_offers_started::INT,
  measure__special_offers_finished::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts