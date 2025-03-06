MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__shift)
);

SELECT
  *
  EXCLUDE (_hook__reference__shift)
FROM silver.bag__adventure_works__shifts