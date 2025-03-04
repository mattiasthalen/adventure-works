MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  job_candidate_id::BIGINT,
  business_entity_id::BIGINT,
  modified_date::VARCHAR,
  resume::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__job_candidates"
);
