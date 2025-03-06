MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__person__sales)
);

SELECT
  *
  EXCLUDE (_hook__person__sales)
FROM silver.bag__adventure_works__sales_person_quota_histories