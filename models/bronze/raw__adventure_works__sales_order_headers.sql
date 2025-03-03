MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  sales_order_id,
  account_number,
  bill_to_address_id,
  credit_card_approval_code,
  credit_card_id,
  currency_rate_id,
  customer_id,
  due_date,
  freight,
  modified_date,
  online_order_flag,
  order_date,
  purchase_order_number,
  revision_number,
  rowguid,
  sales_order_number,
  sales_person_id,
  ship_date,
  ship_method_id,
  ship_to_address_id,
  status,
  sub_total,
  tax_amt,
  territory_id,
  total_due,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_order_headers"
)
