MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__product_photo)
);

SELECT
  *
  EXCLUDE (_hook__reference__product_photo)
FROM dab.bag__adventure_works__product_photos