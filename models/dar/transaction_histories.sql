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

WITH cte__source AS (
  SELECT
    _pit_hook__transaction_history,
    transaction_history__transaction_id,
    transaction_history__product_id,
    transaction_history__reference_order_id,
    transaction_history__reference_order_line_id,
    transaction_history__transaction_date,
    transaction_history__transaction_type,
    transaction_history__quantity,
    transaction_history__actual_cost,
    transaction_history__modified_date,
    transaction_history__record_loaded_at,
    transaction_history__record_updated_at,
    transaction_history__record_version,
    transaction_history__record_valid_from,
    transaction_history__record_valid_to,
    transaction_history__is_current_record
  FROM dab.bag__adventure_works__transaction_histories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__transaction_history,
    NULL AS transaction_history__transaction_id,
    NULL AS transaction_history__product_id,
    NULL AS transaction_history__reference_order_id,
    NULL AS transaction_history__reference_order_line_id,
    NULL AS transaction_history__transaction_date,
    'N/A' AS transaction_history__transaction_type,
    NULL AS transaction_history__quantity,
    NULL AS transaction_history__actual_cost,
    NULL AS transaction_history__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS transaction_history__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS transaction_history__record_updated_at,
    0 AS transaction_history__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS transaction_history__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS transaction_history__record_valid_to,
    TRUE AS transaction_history__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__transaction_history::BLOB,
  transaction_history__transaction_id::BIGINT,
  transaction_history__product_id::BIGINT,
  transaction_history__reference_order_id::BIGINT,
  transaction_history__reference_order_line_id::BIGINT,
  transaction_history__transaction_date::DATE,
  transaction_history__transaction_type::TEXT,
  transaction_history__quantity::BIGINT,
  transaction_history__actual_cost::DOUBLE,
  transaction_history__modified_date::DATE,
  transaction_history__record_loaded_at::TIMESTAMP,
  transaction_history__record_updated_at::TIMESTAMP,
  transaction_history__record_version::TEXT,
  transaction_history__record_valid_from::TIMESTAMP,
  transaction_history__record_valid_to::TIMESTAMP,
  transaction_history__is_current_record::BOOLEAN
FROM cte__final