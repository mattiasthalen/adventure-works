MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__transaction_history)
);

SELECT
  *
  EXCLUDE (_hook__transaction_history, _hook__product, _hook__order__reference)
FROM silver.bag__adventure_works__transaction_histories