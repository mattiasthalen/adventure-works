MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__job_candidate),
  description 'Business viewpoint of job_candidates data: Résumés submitted to Human Resources by job applicants.',
  column_descriptions (
    job_candidate__job_candidate_id = 'Primary key for JobCandidate records.',
    job_candidate__resume = 'Résumé in XML format.',
    job_candidate__business_entity_id = 'Employee identification number if applicant was hired. Foreign key to Employee.BusinessEntityID.',
    job_candidate__modified_date = 'Date when this record was last modified',
    job_candidate__record_loaded_at = 'Timestamp when this record was loaded into the system',
    job_candidate__record_updated_at = 'Timestamp when this record was last updated',
    job_candidate__record_version = 'Version number for this record',
    job_candidate__record_valid_from = 'Timestamp from which this record version is valid',
    job_candidate__record_valid_to = 'Timestamp until which this record version is valid',
    job_candidate__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__job_candidate, _hook__person__employee)
FROM dab.bag__adventure_works__job_candidates