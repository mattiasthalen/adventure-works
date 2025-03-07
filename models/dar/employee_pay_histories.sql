MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__employee)
);

SELECT
  *
  EXCLUDE (_hook__person__employee)
FROM dab.bag__adventure_works__employee_pay_histories