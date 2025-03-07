MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__phone_number_type)
);

SELECT
  *
  EXCLUDE (_hook__reference__phone_number_type)
FROM dab.bag__adventure_works__phone_number_types