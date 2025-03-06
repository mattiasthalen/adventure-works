MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__reference__product_model)
);

SELECT
  *
  EXCLUDE (_hook__reference__product_model)
FROM silver.bag__adventure_works__product_models