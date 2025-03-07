MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    sales_order_id::BIGINT,
    revision_number::BIGINT,
    order_date::TEXT,
    due_date::TEXT,
    ship_date::TEXT,
    status::BIGINT,
    online_order_flag::BOOLEAN,
    sales_order_number::TEXT,
    purchase_order_number::TEXT,
    account_number::TEXT,
    customer_id::BIGINT,
    sales_person_id::BIGINT,
    territory_id::BIGINT,
    bill_to_address_id::BIGINT,
    ship_to_address_id::BIGINT,
    ship_method_id::BIGINT,
    credit_card_id::BIGINT,
    credit_card_approval_code::TEXT,
    sub_total::DOUBLE,
    tax_amt::DOUBLE,
    freight::DOUBLE,
    total_due::DOUBLE,
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    currency_rate_id::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_order_headers"
)
;