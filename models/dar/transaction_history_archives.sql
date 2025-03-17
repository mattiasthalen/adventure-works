MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__transaction_history_archive),
  description 'Business viewpoint of transaction_history_archives data: Transactions for previous years.',
  column_descriptions (
    transaction_history_archive__transaction_id = 'Primary key for TransactionHistoryArchive records.',
    transaction_history_archive__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    transaction_history_archive__reference_order_id = 'Purchase order, sales order, or work order identification number.',
    transaction_history_archive__reference_order_line_id = 'Line number associated with the purchase order, sales order, or work order.',
    transaction_history_archive__transaction_date = 'Date and time of the transaction.',
    transaction_history_archive__transaction_type = 'W = Work Order, S = Sales Order, P = Purchase Order.',
    transaction_history_archive__quantity = 'Product quantity.',
    transaction_history_archive__actual_cost = 'Product cost.',
    transaction_history_archive__modified_date = 'Date when this record was last modified',
    transaction_history_archive__record_loaded_at = 'Timestamp when this record was loaded into the system',
    transaction_history_archive__record_updated_at = 'Timestamp when this record was last updated',
    transaction_history_archive__record_version = 'Version number for this record',
    transaction_history_archive__record_valid_from = 'Timestamp from which this record version is valid',
    transaction_history_archive__record_valid_to = 'Timestamp until which this record version is valid',
    transaction_history_archive__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__transaction_history_archive, _hook__product, _hook__order__reference)
FROM dab.bag__adventure_works__transaction_history_archives