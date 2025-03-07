MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order_line__sales)
);

SELECT
  *
  EXCLUDE (_hook__order_line__sales, _hook__order__sales, _hook__product, _hook__reference__special_offer)
FROM dab.bag__adventure_works__sales_order_details