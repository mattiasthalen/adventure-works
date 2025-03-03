MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  shopping_cart_item_id,
  date_created,
  modified_date,
  product_id,
  quantity,
  shopping_cart_id,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__shopping_cart_items"
)
