MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__person__individual)
);

SELECT
  *
  EXCLUDE (_hook__person__individual)
FROM silver.bag__adventure_works__persons