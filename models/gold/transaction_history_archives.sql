MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__transaction_history_archive)
);

SELECT
  *
  EXCLUDE (_hook__transaction_history_archive, _hook__product, _hook__order__reference)
FROM silver.bag__adventure_works__transaction_history_archives