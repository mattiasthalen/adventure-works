MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_category)
);

SELECT
  *
  EXCLUDE (_hook__product_category)
FROM silver.bag__adventure_works__product_categories