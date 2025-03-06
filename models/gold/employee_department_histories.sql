MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__person__employee)
);

SELECT
  *
  EXCLUDE (_hook__person__employee, _hook__department, _hook__reference__shift)
FROM silver.bag__adventure_works__employee_department_histories