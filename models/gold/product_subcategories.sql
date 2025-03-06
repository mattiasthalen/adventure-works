MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_subcategory)
);

SELECT
  *
  EXCLUDE (_hook__product_subcategory, _hook__product_category)
FROM silver.bag__adventure_works__product_subcategories