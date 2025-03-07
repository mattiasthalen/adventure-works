MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__location)
);

SELECT
  *
  EXCLUDE (_hook__reference__location, _hook__product)
FROM dab.bag__adventure_works__product_inventories