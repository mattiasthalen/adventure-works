MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__scrap_reason)
);

SELECT
  *
  EXCLUDE (_hook__reference__scrap_reason)
FROM silver.bag__adventure_works__scrap_reasons