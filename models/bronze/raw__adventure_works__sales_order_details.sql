MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  sales_order_detail_id,
  carrier_tracking_number,
  line_total,
  modified_date,
  order_qty,
  product_id,
  rowguid,
  sales_order_id,
  special_offer_id,
  unit_price,
  unit_price_discount,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_order_details"
)
