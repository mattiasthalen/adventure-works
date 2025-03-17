MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_vendors data: Cross-reference table mapping vendors with the products they supply.',
  column_descriptions (
    product_id = 'Primary key. Foreign key to Product.ProductID.',
    business_entity_id = 'Primary key. Foreign key to Vendor.BusinessEntityID.',
    average_lead_time = 'The average span of time (in days) between placing an order with the vendor and receiving the purchased product.',
    standard_price = 'The vendor''s usual selling price.',
    last_receipt_cost = 'The selling price when last purchased.',
    last_receipt_date = 'Date the product was last received by the vendor.',
    min_order_qty = 'The minimum quantity that should be ordered.',
    max_order_qty = 'The maximum quantity that should be ordered.',
    unit_measure_code = 'The product''s unit of measure.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    on_order_qty = 'The quantity currently on order.'
  )
);

SELECT
    product_id::BIGINT,
    business_entity_id::BIGINT,
    average_lead_time::BIGINT,
    standard_price::DOUBLE,
    last_receipt_cost::DOUBLE,
    last_receipt_date::DATE,
    min_order_qty::BIGINT,
    max_order_qty::BIGINT,
    unit_measure_code::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    on_order_qty::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_vendors"
)
;