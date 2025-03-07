MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__address)
);

SELECT
  *
  EXCLUDE (_hook__address, _hook__reference__state_province)
FROM dab.bag__adventure_works__addresses