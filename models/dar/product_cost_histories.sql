MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_cost_history)
);

SELECT
  *
  EXCLUDE (_hook__product_cost_history, _hook__product, _hook__epoch__start_date)
FROM dab.bag__adventure_works__product_cost_histories