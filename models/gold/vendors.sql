MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__vendor)
);

SELECT
  *
  EXCLUDE (_hook__vendor)
FROM silver.bag__adventure_works__vendors