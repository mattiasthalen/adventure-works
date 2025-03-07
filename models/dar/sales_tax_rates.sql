MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__sales_tax_rate)
);

SELECT
  *
  EXCLUDE (_hook__reference__sales_tax_rate, _hook__reference__state_province)
FROM dab.bag__adventure_works__sales_tax_rates