MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    transaction_id AS transaction__transaction_id,
    product_id AS transaction__product_id,
    reference_order_id AS transaction__reference_order_id,
    reference_order_line_id AS transaction__reference_order_line_id,
    actual_cost AS transaction__actual_cost,
    modified_date AS transaction__modified_date,
    quantity AS transaction__quantity,
    transaction_date AS transaction__transaction_date,
    transaction_type AS transaction__transaction_type,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS transaction__record_loaded_at
  FROM bronze.raw__adventure_works__transaction_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction__transaction_id ORDER BY transaction__record_loaded_at) AS transaction__record_version,
    CASE
      WHEN transaction__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE transaction__record_loaded_at
    END AS transaction__record_valid_from,
    COALESCE(
      LEAD(transaction__record_loaded_at) OVER (PARTITION BY transaction__transaction_id ORDER BY transaction__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS transaction__record_valid_to,
    transaction__record_valid_to = @max_ts::TIMESTAMP AS transaction__is_current_record,
    CASE
      WHEN transaction__is_current_record
      THEN transaction__record_loaded_at
      ELSE transaction__record_valid_to
    END AS transaction__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'transaction|adventure_works|',
      transaction__transaction_id,
      '~epoch|valid_from|',
      transaction__record_valid_from
    ) AS _pit_hook__transaction,
    CONCAT('transaction|adventure_works|', transaction__transaction_id) AS _hook__transaction,
    CONCAT('product|adventure_works|', transaction__product_id) AS _hook__product,
    CONCAT('reference_order|adventure_works|', transaction__reference_order_id) AS _hook__reference_order,
    CONCAT('reference_order_line|adventure_works|', transaction__reference_order_line_id) AS _hook__reference_order_line,
    *
  FROM validity
)
SELECT
  _pit_hook__transaction::BLOB,
  _hook__transaction::BLOB,
  _hook__product::BLOB,
  _hook__reference_order::BLOB,
  _hook__reference_order_line::BLOB,
  transaction__transaction_id::VARCHAR,
  transaction__product_id::VARCHAR,
  transaction__reference_order_id::VARCHAR,
  transaction__reference_order_line_id::VARCHAR,
  transaction__actual_cost::VARCHAR,
  transaction__modified_date::VARCHAR,
  transaction__quantity::VARCHAR,
  transaction__transaction_date::VARCHAR,
  transaction__transaction_type::VARCHAR,
  transaction__record_loaded_at::TIMESTAMP,
  transaction__record_version::INT,
  transaction__record_valid_from::TIMESTAMP,
  transaction__record_valid_to::TIMESTAMP,
  transaction__is_current_record::BOOLEAN,
  transaction__record_updated_at::TIMESTAMP
FROM hooks