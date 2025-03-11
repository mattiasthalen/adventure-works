MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__employee_department_history)
);

SELECT
  *
  EXCLUDE (_hook__employee_department_history, _hook__person__employee, _hook__department, _hook__reference__shift, _hook__epoch__start_date)
FROM dab.bag__adventure_works__employee_department_histories