MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__sales)
);

SELECT
  *
  EXCLUDE (_hook__person__sales)
FROM dab.bag__adventure_works__sales_person_quota_histories