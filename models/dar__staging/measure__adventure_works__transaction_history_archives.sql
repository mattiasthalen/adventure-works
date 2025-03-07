MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__transaction_history_archive,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__transaction_history_archive, _hook__epoch__date),
  references (_pit_hook__transaction_history_archive, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__transaction_history_archive,
    transaction_history_archive__transaction_date,
    transaction_history_archive__modified_date
  FROM dab.bag__adventure_works__transaction_history_archives
  WHERE 1 = 1
  AND transaction_history_archive__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__transaction_date AS (
  SELECT
    _pit_hook__transaction_history_archive,
    transaction_history_archive__transaction_date::DATE AS measure_date,
    1 AS measure__transaction_history_archives_transaction
  FROM cte__source
  WHERE transaction_history_archive__transaction_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__transaction_history_archive,
    transaction_history_archive__modified_date::DATE AS measure_date,
    1 AS measure__transaction_history_archives_modified
  FROM cte__source
  WHERE transaction_history_archive__modified_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__transaction_date
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__transaction_history_archive, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__transaction_history_archive::BLOB,
  _hook__epoch__date::BLOB,
  measure__transaction_history_archives_transaction::INT,
  measure__transaction_history_archives_modified::INT
FROM cte__epoch