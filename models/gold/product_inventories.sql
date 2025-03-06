MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__reference__location)
);

SELECT
  *
  EXCLUDE (_hook__reference__location, _hook__product)
FROM silver.bag__adventure_works__product_inventories