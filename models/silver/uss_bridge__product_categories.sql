MODEL (
  kind FULL,
  enabled TRUE
);

WITH bridge AS (
  SELECT
    'product_categories' AS stage,
    bag__adventure_works__product_categories._pit_hook__product_category,
    bag__adventure_works__product_categories._hook__product_category,
    bag__adventure_works__product_categories.product_category__record_loaded_at AS bridge__record_loaded_at,
    bag__adventure_works__product_categories.product_category__record_updated_at AS bridge__record_updated_at,
    bag__adventure_works__product_categories.product_category__record_valid_from AS bridge__record_valid_from,
    bag__adventure_works__product_categories.product_category__record_valid_to AS bridge__record_valid_to
  FROM silver.bag__adventure_works__product_categories
)
SELECT
  *,
  bridge__record_valid_to = @MAX_TS::TIMESTAMP AS bridge__is_current_record
FROM bridge