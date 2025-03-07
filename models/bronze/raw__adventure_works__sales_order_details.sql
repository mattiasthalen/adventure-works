MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    sales_order_id::BIGINT,
    sales_order_detail_id::BIGINT,
    carrier_tracking_number::TEXT,
    order_qty::BIGINT,
    product_id::BIGINT,
    special_offer_id::BIGINT,
    unit_price::DOUBLE,
    unit_price_discount::DOUBLE,
    line_total::DOUBLE,
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_order_details"
)
;