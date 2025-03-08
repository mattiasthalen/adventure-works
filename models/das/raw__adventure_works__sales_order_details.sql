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
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__sales_order_details"
)
;