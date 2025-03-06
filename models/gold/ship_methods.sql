MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__ship_method)
);

SELECT
  *
  EXCLUDE (_hook__ship_method)
FROM silver.bag__adventure_works__ship_methods