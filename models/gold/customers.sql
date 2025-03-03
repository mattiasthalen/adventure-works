MODEL (
  kind VIEW,
  enabled FALSE
);

SELECT
  *
  EXCLUDE (_hook__customer, _hook__person, _hook__store, _hook__territory)
FROM silver.bag__adventure_works__customers