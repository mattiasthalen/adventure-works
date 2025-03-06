MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__reference__sales_reason)
);

SELECT
  *
  EXCLUDE (_hook__reference__sales_reason)
FROM silver.bag__adventure_works__sales_reasons