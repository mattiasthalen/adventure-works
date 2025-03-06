MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__currency_rate)
);

SELECT
  *
  EXCLUDE (_hook__currency__from, _hook__currency__to, _hook__currency_rate)
FROM silver.bag__adventure_works__currency_rates