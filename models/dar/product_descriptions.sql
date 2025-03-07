MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__product_description)
);

SELECT
  *
  EXCLUDE (_hook__reference__product_description)
FROM dab.bag__adventure_works__product_descriptions