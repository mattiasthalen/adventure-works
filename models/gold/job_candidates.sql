MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__job_candidate)
);

SELECT
  *
  EXCLUDE (_hook__job_candidate, _hook__person__employee)
FROM silver.bag__adventure_works__job_candidates