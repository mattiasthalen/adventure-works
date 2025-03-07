MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__currency_rate,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__currency_rate, _hook__epoch__date),
  references (_pit_hook__currency_rate, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__currency_rate,
    currency_rate__currency_rate_date
  FROM silver.bag__adventure_works__currency_rates
  WHERE 1 = 1
  AND currency_rate__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__currency_rate_date AS (
  SELECT
    _pit_hook__currency_rate,
    currency_rate__currency_rate_date::DATE AS measure_date,
    1 AS measure__currency_rates_currency_rate
  FROM cte__source
  WHERE currency_rate__currency_rate_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__currency_rate_date
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__currency_rate::BLOB,
  _hook__epoch__date::BLOB,
  measure__currency_rates_currency_rate::INT
FROM cte__epoch