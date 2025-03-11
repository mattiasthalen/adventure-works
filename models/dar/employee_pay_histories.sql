MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__employee_pay_history)
);

SELECT
  *
  EXCLUDE (_hook__employee_pay_history, _hook__person__employee, _hook__epoch__rate_change_date)
FROM dab.bag__adventure_works__employee_pay_histories