MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    job_candidate_id::BIGINT,
    resume::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    business_entity_id::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__job_candidates"
)
;