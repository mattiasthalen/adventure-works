MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__department)
);

SELECT
  *
  EXCLUDE (_hook__department)
FROM dab.bag__adventure_works__departments