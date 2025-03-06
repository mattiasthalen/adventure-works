MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__reference__culture)
);

SELECT
  *
  EXCLUDE (_hook__reference__culture)
FROM silver.bag__adventure_works__cultures