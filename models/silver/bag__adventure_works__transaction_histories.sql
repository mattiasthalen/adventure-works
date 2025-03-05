MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column transaction_history__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__transaction_history, _hook__transaction_history),
  references (_hook__product, _hook__order__reference)
);

WITH staging AS (
  SELECT
    transaction_id AS transaction_history__transaction_id,
    product_id AS transaction_history__product_id,
    reference_order_id AS transaction_history__reference_order_id,
    reference_order_line_id AS transaction_history__reference_order_line_id,
    transaction_date AS transaction_history__transaction_date,
    transaction_type AS transaction_history__transaction_type,
    quantity AS transaction_history__quantity,
    actual_cost AS transaction_history__actual_cost,
    modified_date AS transaction_history__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS transaction_history__record_loaded_at
  FROM bronze.raw__adventure_works__transaction_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction_history__transaction_id ORDER BY transaction_history__record_loaded_at) AS transaction_history__record_version,
    CASE
      WHEN transaction_history__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE transaction_history__record_loaded_at
    END AS transaction_history__record_valid_from,
    COALESCE(
      LEAD(transaction_history__record_loaded_at) OVER (PARTITION BY transaction_history__transaction_id ORDER BY transaction_history__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS transaction_history__record_valid_to,
    transaction_history__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS transaction_history__is_current_record,
    CASE
      WHEN transaction_history__is_current_record
      THEN transaction_history__record_loaded_at
      ELSE transaction_history__record_valid_to
    END AS transaction_history__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product__adventure_works|',
      transaction_history__transaction_id,
      '~epoch|valid_from|',
      transaction_history__record_valid_from
    )::BLOB AS _pit_hook__transaction_history,
    CONCAT('product__adventure_works|', transaction_history__transaction_id) AS _hook__transaction_history,
    CONCAT('product__adventure_works|', transaction_history__product_id) AS _hook__product,
    CONCAT('order__adventure_works|', transaction_history__reference_order_id) AS _hook__order__reference,
    *
  FROM validity
)
SELECT
  _pit_hook__transaction_history::BLOB,
  _hook__transaction_history::BLOB,
  _hook__product::BLOB,
  _hook__order__reference::BLOB,
  transaction_history__transaction_id::BIGINT,
  transaction_history__product_id::BIGINT,
  transaction_history__reference_order_id::BIGINT,
  transaction_history__reference_order_line_id::BIGINT,
  transaction_history__transaction_date::TEXT,
  transaction_history__transaction_type::TEXT,
  transaction_history__quantity::BIGINT,
  transaction_history__actual_cost::DOUBLE,
  transaction_history__modified_date::DATE,
  transaction_history__record_loaded_at::TIMESTAMP,
  transaction_history__record_updated_at::TIMESTAMP,
  transaction_history__record_version::TEXT,
  transaction_history__record_valid_from::TIMESTAMP,
  transaction_history__record_valid_to::TIMESTAMP,
  transaction_history__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND transaction_history__record_updated_at BETWEEN @start_ts AND @end_ts