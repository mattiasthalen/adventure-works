MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__sales_reason)
);

SELECT
  *
  EXCLUDE (_hook__reference__sales_reason)
FROM dab.bag__adventure_works__sales_reasons