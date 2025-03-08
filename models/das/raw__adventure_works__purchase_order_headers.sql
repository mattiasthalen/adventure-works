MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    purchase_order_id::BIGINT,
    revision_number::BIGINT,
    status::BIGINT,
    employee_id::BIGINT,
    vendor_id::BIGINT,
    ship_method_id::BIGINT,
    order_date::DATE,
    ship_date::DATE,
    sub_total::DOUBLE,
    tax_amt::DOUBLE,
    freight::DOUBLE,
    total_due::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__purchase_order_headers"
)
;