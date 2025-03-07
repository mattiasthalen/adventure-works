MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    product_id::BIGINT,
    business_entity_id::BIGINT,
    average_lead_time::BIGINT,
    standard_price::DOUBLE,
    last_receipt_cost::DOUBLE,
    last_receipt_date::TEXT,
    min_order_qty::BIGINT,
    max_order_qty::BIGINT,
    unit_measure_code::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    on_order_qty::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_vendors"
)
;