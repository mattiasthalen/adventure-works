MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column currency_rate__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__currency_rate)
FROM silver.bag__adventure_works__currency_rates
WHERE 1 = 1
AND currency_rate__record_updated_at BETWEEN @start_ts AND @end_ts