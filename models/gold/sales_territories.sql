MODEL (
  kind VIEW,
  enabled FALSE
);

SELECT
  *
  EXCLUDE (_hook__territory)
FROM silver.bag__adventure_works__sales_territories