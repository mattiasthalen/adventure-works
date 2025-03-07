MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_review)
);

SELECT
  *
  EXCLUDE (_hook__product_review, _hook__product)
FROM silver.bag__adventure_works__product_reviews