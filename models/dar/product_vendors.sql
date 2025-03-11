MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_vendor)
);

SELECT
  *
  EXCLUDE (_hook__product_vendor, _hook__vendor, _hook__product, _hook__reference__unit_measure)
FROM dab.bag__adventure_works__product_vendors