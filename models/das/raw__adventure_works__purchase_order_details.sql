MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    purchase_order_id::BIGINT,
    purchase_order_detail_id::BIGINT,
    due_date::DATE,
    order_qty::BIGINT,
    product_id::BIGINT,
    unit_price::DOUBLE,
    line_total::DOUBLE,
    received_qty::DOUBLE,
    rejected_qty::DOUBLE,
    stocked_qty::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__purchase_order_details"
)
;