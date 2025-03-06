MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__bill_of_materials)
);

SELECT
  *
  EXCLUDE (_hook__bill_of_materials, _hook__product__assembly, _hook__product__component, _hook__reference__unit_measure)
FROM silver.bag__adventure_works__bill_of_materials