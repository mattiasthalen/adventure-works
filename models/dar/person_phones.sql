MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__individual)
);

SELECT
  *
  EXCLUDE (_hook__person__individual, _hook__reference__phone_number_type)
FROM dab.bag__adventure_works__person_phones