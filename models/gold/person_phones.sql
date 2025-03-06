MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__person__individual)
);

SELECT
  *
  EXCLUDE (_hook__person__individual, _hook__reference__phone_number_type)
FROM silver.bag__adventure_works__person_phones