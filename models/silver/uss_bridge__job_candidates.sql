MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__job_candidate),
  references (_hook__person__employee)
);

SELECT
  'job_candidates' AS peripheral,
  _hook__job_candidate::BLOB,
  _hook__person__employee::BLOB,
  job_candidate__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  job_candidate__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  job_candidate__record_version::TEXT AS bridge__record_version,
  job_candidate__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  job_candidate__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  job_candidate__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__job_candidates
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts