MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__shopping_cart_item)
);

SELECT
  *
  EXCLUDE (_hook__shopping_cart_item, _hook__product)
FROM silver.bag__adventure_works__shopping_cart_items