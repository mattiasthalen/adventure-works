MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column transaction_histori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    transaction_id AS transaction_histori__transaction_id,
    product_id AS transaction_histori__product_id,
    reference_order_id AS transaction_histori__reference_order_id,
    reference_order_line_id AS transaction_histori__reference_order_line_id,
    actual_cost AS transaction_histori__actual_cost,
    modified_date AS transaction_histori__modified_date,
    quantity AS transaction_histori__quantity,
    transaction_date AS transaction_histori__transaction_date,
    transaction_type AS transaction_histori__transaction_type,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS transaction_histori__record_loaded_at
  FROM bronze.raw__adventure_works__transaction_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction_histori__transaction_id ORDER BY transaction_histori__record_loaded_at) AS transaction_histori__record_version,
    CASE
      WHEN transaction_histori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE transaction_histori__record_loaded_at
    END AS transaction_histori__record_valid_from,
    COALESCE(
      LEAD(transaction_histori__record_loaded_at) OVER (PARTITION BY transaction_histori__transaction_id ORDER BY transaction_histori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS transaction_histori__record_valid_to,
    transaction_histori__record_valid_to = @max_ts::TIMESTAMP AS transaction_histori__is_current_record,
    CASE
      WHEN transaction_histori__is_current_record
      THEN transaction_histori__record_loaded_at
      ELSE transaction_histori__record_valid_to
    END AS transaction_histori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'transaction|adventure_works|',
      transaction_histori__transaction_id,
      '~epoch|valid_from|',
      transaction_histori__record_valid_from
    ) AS _pit_hook__transaction,
    CONCAT('transaction|adventure_works|', transaction_histori__transaction_id) AS _hook__transaction,
    CONCAT('product|adventure_works|', transaction_histori__product_id) AS _hook__product,
    CONCAT('reference_order|adventure_works|', transaction_histori__reference_order_id) AS _hook__reference_order,
    CONCAT('reference_order_line|adventure_works|', transaction_histori__reference_order_line_id) AS _hook__reference_order_line,
    *
  FROM validity
)
SELECT
  _pit_hook__transaction::BLOB,
  _hook__transaction::BLOB,
  _hook__product::BLOB,
  _hook__reference_order::BLOB,
  _hook__reference_order_line::BLOB,
  transaction_histori__transaction_id::BIGINT,
  transaction_histori__product_id::BIGINT,
  transaction_histori__reference_order_id::BIGINT,
  transaction_histori__reference_order_line_id::BIGINT,
  transaction_histori__actual_cost::DOUBLE,
  transaction_histori__modified_date::VARCHAR,
  transaction_histori__quantity::BIGINT,
  transaction_histori__transaction_date::VARCHAR,
  transaction_histori__transaction_type::VARCHAR,
  transaction_histori__record_loaded_at::TIMESTAMP,
  transaction_histori__record_updated_at::TIMESTAMP,
  transaction_histori__record_valid_from::TIMESTAMP,
  transaction_histori__record_valid_to::TIMESTAMP,
  transaction_histori__record_version::INT,
  transaction_histori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND transaction_histori__record_updated_at BETWEEN @start_ts AND @end_ts