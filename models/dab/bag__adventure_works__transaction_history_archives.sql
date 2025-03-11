MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__transaction_history_archive
  ),
  tags hook,
  grain (_pit_hook__transaction_history_archive, _hook__transaction_history_archive),
  references (_hook__product, _hook__order__reference)
);

WITH staging AS (
  SELECT
    transaction_id AS transaction_history_archive__transaction_id,
    product_id AS transaction_history_archive__product_id,
    reference_order_id AS transaction_history_archive__reference_order_id,
    reference_order_line_id AS transaction_history_archive__reference_order_line_id,
    transaction_date AS transaction_history_archive__transaction_date,
    transaction_type AS transaction_history_archive__transaction_type,
    quantity AS transaction_history_archive__quantity,
    actual_cost AS transaction_history_archive__actual_cost,
    modified_date AS transaction_history_archive__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS transaction_history_archive__record_loaded_at
  FROM das.raw__adventure_works__transaction_history_archives
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction_history_archive__transaction_id ORDER BY transaction_history_archive__record_loaded_at) AS transaction_history_archive__record_version,
    CASE
      WHEN transaction_history_archive__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE transaction_history_archive__record_loaded_at
    END AS transaction_history_archive__record_valid_from,
    COALESCE(
      LEAD(transaction_history_archive__record_loaded_at) OVER (PARTITION BY transaction_history_archive__transaction_id ORDER BY transaction_history_archive__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS transaction_history_archive__record_valid_to,
    transaction_history_archive__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS transaction_history_archive__is_current_record,
    CASE
      WHEN transaction_history_archive__is_current_record
      THEN transaction_history_archive__record_loaded_at
      ELSE transaction_history_archive__record_valid_to
    END AS transaction_history_archive__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product__adventure_works|', transaction_history_archive__transaction_id) AS _hook__transaction_history_archive,
    CONCAT('product__adventure_works|', transaction_history_archive__product_id) AS _hook__product,
    CONCAT('order__adventure_works|', transaction_history_archive__reference_order_id) AS _hook__order__reference,
    CONCAT_WS('~',
      _hook__transaction_history_archive,
      'epoch__valid_from|'||transaction_history_archive__record_valid_from
    ) AS _pit_hook__transaction_history_archive,
    *
  FROM validity
)
SELECT
  _pit_hook__transaction_history_archive::BLOB,
  _hook__transaction_history_archive::BLOB,
  _hook__product::BLOB,
  _hook__order__reference::BLOB,
  transaction_history_archive__transaction_id::BIGINT,
  transaction_history_archive__product_id::BIGINT,
  transaction_history_archive__reference_order_id::BIGINT,
  transaction_history_archive__reference_order_line_id::BIGINT,
  transaction_history_archive__transaction_date::DATE,
  transaction_history_archive__transaction_type::TEXT,
  transaction_history_archive__quantity::BIGINT,
  transaction_history_archive__actual_cost::DOUBLE,
  transaction_history_archive__modified_date::DATE,
  transaction_history_archive__record_loaded_at::TIMESTAMP,
  transaction_history_archive__record_updated_at::TIMESTAMP,
  transaction_history_archive__record_version::TEXT,
  transaction_history_archive__record_valid_from::TIMESTAMP,
  transaction_history_archive__record_valid_to::TIMESTAMP,
  transaction_history_archive__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND transaction_history_archive__record_updated_at BETWEEN @start_ts AND @end_ts