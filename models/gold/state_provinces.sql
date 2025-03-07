MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__state_province)
);

SELECT
  *
  EXCLUDE (_hook__reference__state_province, _hook__reference__country_region, _hook__territory__sales)
FROM silver.bag__adventure_works__state_provinces