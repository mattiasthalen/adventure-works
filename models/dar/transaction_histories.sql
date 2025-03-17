MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__transaction_history),
  description 'Business viewpoint of transaction_histories data: Record of each purchase order, sales order, or work order transaction year to date.',
  column_descriptions (
    transaction_history__transaction_id = 'Primary key for TransactionHistory records.',
    transaction_history__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    transaction_history__reference_order_id = 'Purchase order, sales order, or work order identification number.',
    transaction_history__reference_order_line_id = 'Line number associated with the purchase order, sales order, or work order.',
    transaction_history__transaction_date = 'Date and time of the transaction.',
    transaction_history__transaction_type = 'W = WorkOrder, S = SalesOrder, P = PurchaseOrder.',
    transaction_history__quantity = 'Product quantity.',
    transaction_history__actual_cost = 'Product cost.',
    transaction_history__modified_date = 'Date when this record was last modified',
    transaction_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    transaction_history__record_updated_at = 'Timestamp when this record was last updated',
    transaction_history__record_version = 'Version number for this record',
    transaction_history__record_valid_from = 'Timestamp from which this record version is valid',
    transaction_history__record_valid_to = 'Timestamp until which this record version is valid',
    transaction_history__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__transaction_history, _hook__product, _hook__order__reference)
FROM dab.bag__adventure_works__transaction_histories