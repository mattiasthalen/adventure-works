MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__reference__product_photo)
);

SELECT
  *
  EXCLUDE (_hook__reference__product_photo)
FROM silver.bag__adventure_works__product_photos