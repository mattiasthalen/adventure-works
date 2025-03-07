MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__department)
);

SELECT
  *
  EXCLUDE (_hook__department)
FROM silver.bag__adventure_works__departments