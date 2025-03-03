MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__ship_method)
FROM silver.bag__adventure_works__ship_methods