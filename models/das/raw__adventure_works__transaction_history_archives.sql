MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of transaction_history_archives data: Transactions for previous years.',
  column_descriptions (
    transaction_id = 'Primary key for TransactionHistoryArchive records.',
    product_id = 'Product identification number. Foreign key to Product.ProductID.',
    reference_order_id = 'Purchase order, sales order, or work order identification number.',
    reference_order_line_id = 'Line number associated with the purchase order, sales order, or work order.',
    transaction_date = 'Date and time of the transaction.',
    transaction_type = 'W = Work Order, S = Sales Order, P = Purchase Order.',
    quantity = 'Product quantity.',
    actual_cost = 'Product cost.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    transaction_id::BIGINT,
    product_id::BIGINT,
    reference_order_id::BIGINT,
    reference_order_line_id::BIGINT,
    transaction_date::DATE,
    transaction_type::TEXT,
    quantity::BIGINT,
    actual_cost::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__transaction_history_archives"
)
;