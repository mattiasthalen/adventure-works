MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_model_illustration)
);

SELECT
  *
  EXCLUDE (_hook__product_model_illustration, _hook__reference__illustration, _hook__reference__product_model)
FROM dab.bag__adventure_works__product_model_illustrations