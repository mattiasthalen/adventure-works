MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__address)
);

SELECT
  *
  EXCLUDE (_hook__address, _hook__reference__state_province)
FROM silver.bag__adventure_works__addresses