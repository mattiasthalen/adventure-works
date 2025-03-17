MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of shopping_cart_items data: Contains online customer orders until the order is submitted or cancelled.',
  column_descriptions (
    shopping_cart_item_id = 'Primary key for ShoppingCartItem records.',
    shopping_cart_id = 'Shopping cart identification number.',
    quantity = 'Product quantity ordered.',
    product_id = 'Product ordered. Foreign key to Product.ProductID.',
    date_created = 'Date the time the record was created.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    shopping_cart_item_id::BIGINT,
    shopping_cart_id::TEXT,
    quantity::BIGINT,
    product_id::BIGINT,
    date_created::DATE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__shopping_cart_items"
)
;