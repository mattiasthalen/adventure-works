MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__vendor)
);

SELECT
  *
  EXCLUDE (_hook__vendor)
FROM dab.bag__adventure_works__vendors