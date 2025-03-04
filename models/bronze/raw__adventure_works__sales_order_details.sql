MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  sales_order_detail_id::BIGINT,
  product_id::BIGINT,
  sales_order_id::BIGINT,
  special_offer_id::BIGINT,
  carrier_tracking_number::VARCHAR,
  line_total::DOUBLE,
  modified_date::VARCHAR,
  order_qty::BIGINT,
  rowguid::VARCHAR,
  unit_price::DOUBLE,
  unit_price_discount::DOUBLE,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_order_details"
);
