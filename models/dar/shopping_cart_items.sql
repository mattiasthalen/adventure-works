MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__shopping_cart_item),
  description 'Business viewpoint of shopping_cart_items data: Contains online customer orders until the order is submitted or cancelled.',
  column_descriptions (
    shopping_cart_item__shopping_cart_item_id = 'Primary key for ShoppingCartItem records.',
    shopping_cart_item__shopping_cart_id = 'Shopping cart identification number.',
    shopping_cart_item__quantity = 'Product quantity ordered.',
    shopping_cart_item__product_id = 'Product ordered. Foreign key to Product.ProductID.',
    shopping_cart_item__date_created = 'Date the time the record was created.',
    shopping_cart_item__modified_date = 'Date when this record was last modified',
    shopping_cart_item__record_loaded_at = 'Timestamp when this record was loaded into the system',
    shopping_cart_item__record_updated_at = 'Timestamp when this record was last updated',
    shopping_cart_item__record_version = 'Version number for this record',
    shopping_cart_item__record_valid_from = 'Timestamp from which this record version is valid',
    shopping_cart_item__record_valid_to = 'Timestamp until which this record version is valid',
    shopping_cart_item__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__shopping_cart_item,
    shopping_cart_item__shopping_cart_item_id,
    shopping_cart_item__shopping_cart_id,
    shopping_cart_item__quantity,
    shopping_cart_item__product_id,
    shopping_cart_item__date_created,
    shopping_cart_item__modified_date,
    shopping_cart_item__record_loaded_at,
    shopping_cart_item__record_updated_at,
    shopping_cart_item__record_version,
    shopping_cart_item__record_valid_from,
    shopping_cart_item__record_valid_to,
    shopping_cart_item__is_current_record
  FROM dab.bag__adventure_works__shopping_cart_items
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__shopping_cart_item,
    NULL AS shopping_cart_item__shopping_cart_item_id,
    'N/A' AS shopping_cart_item__shopping_cart_id,
    NULL AS shopping_cart_item__quantity,
    NULL AS shopping_cart_item__product_id,
    NULL AS shopping_cart_item__date_created,
    NULL AS shopping_cart_item__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS shopping_cart_item__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS shopping_cart_item__record_updated_at,
    0 AS shopping_cart_item__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS shopping_cart_item__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS shopping_cart_item__record_valid_to,
    TRUE AS shopping_cart_item__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__shopping_cart_item::BLOB,
  shopping_cart_item__shopping_cart_item_id::BIGINT,
  shopping_cart_item__shopping_cart_id::TEXT,
  shopping_cart_item__quantity::BIGINT,
  shopping_cart_item__product_id::BIGINT,
  shopping_cart_item__date_created::DATE,
  shopping_cart_item__modified_date::DATE,
  shopping_cart_item__record_loaded_at::TIMESTAMP,
  shopping_cart_item__record_updated_at::TIMESTAMP,
  shopping_cart_item__record_version::TEXT,
  shopping_cart_item__record_valid_from::TIMESTAMP,
  shopping_cart_item__record_valid_to::TIMESTAMP,
  shopping_cart_item__is_current_record::BOOLEAN
FROM cte__final