MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__individual)
);

SELECT
  *
  EXCLUDE (_hook__person__individual, _hook__email_address)
FROM silver.bag__adventure_works__email_addresses