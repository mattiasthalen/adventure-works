MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__culture)
);

SELECT
  *
  EXCLUDE (_hook__reference__culture)
FROM dab.bag__adventure_works__cultures