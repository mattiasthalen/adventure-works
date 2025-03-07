MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product)
);

SELECT
  *
  EXCLUDE (_hook__product, _hook__product_subcategory, _hook__reference__product_model)
FROM dab.bag__adventure_works__products