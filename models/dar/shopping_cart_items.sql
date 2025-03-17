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

SELECT
  *
  EXCLUDE (_hook__shopping_cart_item, _hook__product)
FROM dab.bag__adventure_works__shopping_cart_items