MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__product_model)
);

SELECT
  *
  EXCLUDE (_hook__reference__product_model)
FROM dab.bag__adventure_works__product_models