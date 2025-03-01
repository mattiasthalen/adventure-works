MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  enabled TRUE
);

WITH bridge AS (
  SELECT
    'currency_rates' AS stage,
    bag__adventure_works__currency_rates._pit_hook__currency_rate,
    bag__adventure_works__currency_rates._hook__currency_rate,
    bag__adventure_works__currency_rates.currency_rate__record_loaded_at AS bridge__record_loaded_at,
    bag__adventure_works__currency_rates.currency_rate__record_updated_at AS bridge__record_updated_at,
    bag__adventure_works__currency_rates.currency_rate__record_valid_from AS bridge__record_valid_from,
    bag__adventure_works__currency_rates.currency_rate__record_valid_to AS bridge__record_valid_to
  FROM silver.bag__adventure_works__currency_rates
)
SELECT
  *,
  bridge__record_valid_to = @MAX_TS::TIMESTAMP AS bridge__is_current_record
FROM bridge
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts