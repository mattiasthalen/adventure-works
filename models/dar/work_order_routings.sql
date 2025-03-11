MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__work_order_routing)
);

SELECT
  *
  EXCLUDE (_hook__work_order_routing, _hook__order_line__work, _hook__order__work, _hook__product, _hook__reference__location)
FROM dab.bag__adventure_works__work_order_routings