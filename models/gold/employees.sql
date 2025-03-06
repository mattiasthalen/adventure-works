MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__person__employee)
);

SELECT
  *
  EXCLUDE (_hook__person__employee)
FROM silver.bag__adventure_works__employees