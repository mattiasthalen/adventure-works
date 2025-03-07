MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__customer)
);

SELECT
  *
  EXCLUDE (_hook__customer, _hook__person__customer, _hook__store, _hook__territory__sales)
FROM dab.bag__adventure_works__customers