MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__product),
  references (_pit_hook__product_subcategory, _pit_hook__product_category)
);

WITH bridge AS (
  SELECT
    'products' AS stage,
    bag__adventure_works__products._pit_hook__product,
    bag__adventure_works__product_subcategories._pit_hook__product_subcategory,
    bag__adventure_works__product_categories._pit_hook__product_category,
    GREATEST(
      bag__adventure_works__products.product__record_loaded_at,
      bag__adventure_works__product_subcategories.product_subcategory__record_loaded_at,
      bag__adventure_works__product_categories.product_category__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
      bag__adventure_works__products.product__record_updated_at,
      bag__adventure_works__product_subcategories.product_subcategory__record_updated_at,
      bag__adventure_works__product_categories.product_category__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
      bag__adventure_works__products.product__record_valid_from,
      bag__adventure_works__product_subcategories.product_subcategory__record_valid_from,
      bag__adventure_works__product_categories.product_category__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
      bag__adventure_works__products.product__record_valid_to,
      bag__adventure_works__product_subcategories.product_subcategory__record_valid_to,
      bag__adventure_works__product_categories.product_category__record_valid_to
    ) AS bridge__record_valid_to
  FROM silver.bag__adventure_works__products
  LEFT JOIN silver.bag__adventure_works__product_subcategories
    ON bag__adventure_works__products._hook__product_subcategory = bag__adventure_works__product_subcategories._hook__product_subcategory
    AND bag__adventure_works__products.product__record_valid_from <= bag__adventure_works__product_subcategories.product_subcategory__record_valid_to
    AND bag__adventure_works__products.product__record_valid_to >= bag__adventure_works__product_subcategories.product_subcategory__record_valid_from
  LEFT JOIN silver.bag__adventure_works__product_categories
    ON bag__adventure_works__product_subcategories._hook__product_category = bag__adventure_works__product_categories._hook__product_category
    AND bag__adventure_works__products.product__record_valid_from <= bag__adventure_works__product_categories.product_category__record_valid_to
    AND bag__adventure_works__products.product__record_valid_to >= bag__adventure_works__product_categories.product_category__record_valid_from
), final AS (
  SELECT
    *,
    bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
  FROM bridge
)

SELECT
  stage::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__product_category::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::TEXT
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts