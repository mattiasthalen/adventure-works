MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__address_type)
);

SELECT
  *
  EXCLUDE (_hook__reference__address_type)
FROM silver.bag__adventure_works__address_types