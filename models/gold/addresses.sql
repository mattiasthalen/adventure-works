MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__address, _hook__state_province)
FROM silver.bag__adventure_works__addresses