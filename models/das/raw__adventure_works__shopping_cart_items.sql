MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    shopping_cart_item_id::BIGINT,
    shopping_cart_id::TEXT,
    quantity::BIGINT,
    product_id::BIGINT,
    date_created::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__shopping_cart_items"
)
;