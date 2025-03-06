MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order_line__purchase)
);

SELECT
  *
  EXCLUDE (_hook__order_line__purchase, _hook__order__purchase, _hook__product)
FROM silver.bag__adventure_works__purchase_order_details