MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__territory__sales)
);

SELECT
  *
  EXCLUDE (_hook__territory__sales, _hook__reference__country_region)
FROM dab.bag__adventure_works__sales_territories