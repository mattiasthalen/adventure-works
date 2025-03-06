MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__currency)
);

SELECT
  *
  EXCLUDE (_hook__currency)
FROM silver.bag__adventure_works__currencies