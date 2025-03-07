MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__ship_method)
);

SELECT
  *
  EXCLUDE (_hook__ship_method)
FROM dab.bag__adventure_works__ship_methods