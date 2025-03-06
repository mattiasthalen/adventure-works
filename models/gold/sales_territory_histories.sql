MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__sales)
);

SELECT
  *
  EXCLUDE (_hook__person__sales, _hook__territory__sales)
FROM silver.bag__adventure_works__sales_territory_histories