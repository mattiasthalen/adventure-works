MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__store)
);

SELECT
  *
  EXCLUDE (_hook__store, _hook__person__sales)
FROM silver.bag__adventure_works__stores