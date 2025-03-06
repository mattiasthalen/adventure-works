MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order_line__work)
);

SELECT
  *
  EXCLUDE (_hook__order_line__work, _hook__order__work, _hook__product, _hook__reference__location)
FROM silver.bag__adventure_works__work_order_routings