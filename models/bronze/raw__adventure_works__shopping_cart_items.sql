MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  shopping_cart_item_id::BIGINT,
  product_id::BIGINT,
  shopping_cart_id::VARCHAR,
  date_created::VARCHAR,
  modified_date::VARCHAR,
  quantity::BIGINT,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__shopping_cart_items"
);
