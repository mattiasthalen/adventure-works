MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__reference__location)
);

SELECT
  *
  EXCLUDE (_hook__reference__location)
FROM silver.bag__adventure_works__locations