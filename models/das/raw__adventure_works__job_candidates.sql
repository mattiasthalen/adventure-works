MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of job_candidates data: Résumés submitted to Human Resources by job applicants.',
  column_descriptions (
    job_candidate_id = 'Primary key for JobCandidate records.',
    resume = 'Résumé in XML format.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    business_entity_id = 'Employee identification number if applicant was hired. Foreign key to Employee.BusinessEntityID.'
  )
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