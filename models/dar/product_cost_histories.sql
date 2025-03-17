MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_cost_history),
  description 'Business viewpoint of product_cost_histories data: Changes in the cost of a product over time.',
  column_descriptions (
    product_cost_history__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    product_cost_history__start_date = 'Product cost start date.',
    product_cost_history__end_date = 'Product cost end date.',
    product_cost_history__standard_cost = 'Standard cost of the product.',
    product_cost_history__modified_date = 'Date when this record was last modified',
    product_cost_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_cost_history__record_updated_at = 'Timestamp when this record was last updated',
    product_cost_history__record_version = 'Version number for this record',
    product_cost_history__record_valid_from = 'Timestamp from which this record version is valid',
    product_cost_history__record_valid_to = 'Timestamp until which this record version is valid',
    product_cost_history__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__product_cost_history, _hook__product, _hook__epoch__start_date)
FROM dab.bag__adventure_works__product_cost_histories