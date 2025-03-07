MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__transaction_history,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__transaction_history, _hook__epoch__date),
  references (_pit_hook__transaction_history, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__transaction_history,
    transaction_history__transaction_date
  FROM silver.bag__adventure_works__transaction_histories
  WHERE 1 = 1
  AND transaction_history__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__transaction_date AS (
  SELECT
    _pit_hook__transaction_history,
    transaction_history__transaction_date::DATE AS measure_date,
    1 AS measure__transaction_histories_transaction
  FROM cte__source
  WHERE transaction_history__transaction_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__transaction_date
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__transaction_history::BLOB,
  _hook__epoch__date::BLOB,
  measure__transaction_histories_transaction::INT
FROM cte__epoch