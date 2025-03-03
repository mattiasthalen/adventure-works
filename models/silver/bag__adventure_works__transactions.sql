MODEL (
  kind VIEW,
  enabled TRUE
);

WITH cte__union AS (
  SELECT
    *
  FROM bronze.raw__adventure_works__transaction_histories
  UNION ALL
  SELECT
    *
  FROM bronze.raw__adventure_works__transaction_history_archives
), cte__staging AS (
  SELECT
    transaction_id AS transaction__transaction_id,
    product_id AS transaction__product_id,
    reference_order_id AS transaction__reference_order_id,
    reference_order_line_id AS transaction__reference_order_line_id,
    transaction_date::DATE AS transaction__transaction_date,
    transaction_type AS transaction__transaction_type,
    quantity AS transaction__quantity,
    actual_cost AS transaction__actual_cost,
    modified_date AS transaction__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS transaction__record_loaded_at
  FROM cte__union
), cte__validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction__transaction_id ORDER BY transaction__record_loaded_at) AS transaction__record_version,
    CASE
      WHEN transaction__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE transaction__record_loaded_at
    END AS transaction__record_valid_from,
    COALESCE(
      LEAD(transaction__record_loaded_at) OVER (PARTITION BY transaction__transaction_id ORDER BY transaction__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS transaction__record_valid_to,
    transaction__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS transaction__is_current_record,
    CASE
      WHEN transaction__is_current_record
      THEN transaction__record_loaded_at
      ELSE transaction__record_valid_to
    END AS transaction__record_updated_at
  FROM cte__staging
), cte__hooks AS (
  SELECT
    CONCAT(
      'transaction|adventure_works|',
      transaction__transaction_id,
      '~epoch|valid_from|',
      transaction__record_valid_from
    )::BLOB AS _pit_hook__transaction,
    CONCAT('transaction|adventure_works|', transaction__transaction_id)::BLOB AS _hook__transaction,
    CONCAT('product|adventure_works|', transaction__product_id)::BLOB AS _hook__product,
    CONCAT('order|adventure_works|', transaction__reference_order_id)::BLOB AS _hook__order,
    CONCAT('order_line|adventure_works|', transaction__reference_order_line_id)::BLOB AS _hook__order_line,
    *
  FROM cte__validity
)
SELECT
  *
FROM cte__hooks