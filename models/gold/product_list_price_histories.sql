MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product)
);

SELECT
  *
  EXCLUDE (_hook__product)
FROM silver.bag__adventure_works__product_list_price_histories