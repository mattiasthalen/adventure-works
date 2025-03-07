MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__country_region)
);

SELECT
  *
  EXCLUDE (_hook__reference__country_region)
FROM dab.bag__adventure_works__country_regions