MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__currency_rate)
FROM silver.bag__adventure_works__currency_rates