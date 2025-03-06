MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order__sales)
);

SELECT
  *
  EXCLUDE (_hook__order__sales, _hook__customer, _hook__person__sales, _hook__territory__sales, _hook__address__billing, _hook__address__shipping, _hook__ship_method, _hook__credit_card, _hook__currency)
FROM silver.bag__adventure_works__sales_order_headers