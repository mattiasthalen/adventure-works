MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_vendor),
  description 'Business viewpoint of product_vendors data: Cross-reference table mapping vendors with the products they supply.',
  column_descriptions (
    product_vendor__product_id = 'Primary key. Foreign key to Product.ProductID.',
    product_vendor__business_entity_id = 'Primary key. Foreign key to Vendor.BusinessEntityID.',
    product_vendor__average_lead_time = 'The average span of time (in days) between placing an order with the vendor and receiving the purchased product.',
    product_vendor__standard_price = 'The vendor''s usual selling price.',
    product_vendor__last_receipt_cost = 'The selling price when last purchased.',
    product_vendor__last_receipt_date = 'Date the product was last received by the vendor.',
    product_vendor__min_order_qty = 'The minimum quantity that should be ordered.',
    product_vendor__max_order_qty = 'The maximum quantity that should be ordered.',
    product_vendor__unit_measure_code = 'The product''s unit of measure.',
    product_vendor__on_order_qty = 'The quantity currently on order.',
    product_vendor__modified_date = 'Date when this record was last modified',
    product_vendor__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_vendor__record_updated_at = 'Timestamp when this record was last updated',
    product_vendor__record_version = 'Version number for this record',
    product_vendor__record_valid_from = 'Timestamp from which this record version is valid',
    product_vendor__record_valid_to = 'Timestamp until which this record version is valid',
    product_vendor__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__product_vendor, _hook__vendor, _hook__product, _hook__reference__unit_measure)
FROM dab.bag__adventure_works__product_vendors