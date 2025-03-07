MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__special_offer,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__reference__special_offer, _hook__epoch__date),
  references (_pit_hook__reference__special_offer, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__special_offer,
    special_offer__start_date,
    special_offer__end_date
  FROM silver.bag__adventure_works__special_offers
  WHERE 1 = 1
  AND special_offer__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__start_date AS (
  SELECT
    _pit_hook__reference__special_offer,
    special_offer__start_date::DATE AS measure_date,
    1 AS measure__special_offers_started
  FROM cte__source
  WHERE special_offer__start_date IS NOT NULL
), cte__end_date AS (
  SELECT
    _pit_hook__reference__special_offer,
    special_offer__end_date::DATE AS measure_date,
    1 AS measure__special_offers_finished
  FROM cte__source
  WHERE special_offer__end_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__start_date
  FULL OUTER JOIN cte__end_date USING (_pit_hook__reference__special_offer, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__reference__special_offer::BLOB,
  _hook__epoch__date::BLOB,
  measure__special_offers_started::INT,
  measure__special_offers_finished::INT
FROM cte__epoch