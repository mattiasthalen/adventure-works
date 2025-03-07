MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order__purchase)
);

SELECT
  *
  EXCLUDE (_hook__order__purchase, _hook__person__employee, _hook__vendor, _hook__ship_method)
FROM silver.bag__adventure_works__purchase_order_headers