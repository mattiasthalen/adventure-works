MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  job_candidate_id,
  business_entity_id,
  modified_date,
  resume,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__job_candidates"
)
