MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__unit_measure)
);

SELECT
  *
  EXCLUDE (_hook__reference__unit_measure)
FROM dab.bag__adventure_works__unit_measures