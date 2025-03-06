MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__order__work)
);

SELECT
  *
  EXCLUDE (_hook__order__work, _hook__product, _hook__reference__scrap_reason)
FROM silver.bag__adventure_works__work_orders