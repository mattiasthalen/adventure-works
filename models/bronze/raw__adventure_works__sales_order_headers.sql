MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  sales_order_id::BIGINT,
  bill_to_address_id::BIGINT,
  credit_card_id::BIGINT,
  currency_rate_id::BIGINT,
  customer_id::BIGINT,
  sales_person_id::BIGINT,
  ship_method_id::BIGINT,
  ship_to_address_id::BIGINT,
  territory_id::BIGINT,
  account_number::VARCHAR,
  credit_card_approval_code::VARCHAR,
  due_date::VARCHAR,
  freight::DOUBLE,
  modified_date::VARCHAR,
  online_order_flag::BOOLEAN,
  order_date::VARCHAR,
  purchase_order_number::VARCHAR,
  revision_number::BIGINT,
  rowguid::VARCHAR,
  sales_order_number::VARCHAR,
  ship_date::VARCHAR,
  status::BIGINT,
  sub_total::DOUBLE,
  tax_amt::DOUBLE,
  total_due::DOUBLE,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_order_headers"
);
