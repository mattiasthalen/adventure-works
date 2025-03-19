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

WITH cte__source AS (
  SELECT
    _pit_hook__job_candidate,
    job_candidate__job_candidate_id,
    job_candidate__resume,
    job_candidate__business_entity_id,
    job_candidate__modified_date,
    job_candidate__record_loaded_at,
    job_candidate__record_updated_at,
    job_candidate__record_version,
    job_candidate__record_valid_from,
    job_candidate__record_valid_to,
    job_candidate__is_current_record
  FROM dab.bag__adventure_works__job_candidates
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__job_candidate,
    NULL AS job_candidate__job_candidate_id,
    'N/A' AS job_candidate__resume,
    NULL AS job_candidate__business_entity_id,
    NULL AS job_candidate__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS job_candidate__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS job_candidate__record_updated_at,
    0 AS job_candidate__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS job_candidate__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS job_candidate__record_valid_to,
    TRUE AS job_candidate__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__job_candidate::BLOB,
  job_candidate__job_candidate_id::BIGINT,
  job_candidate__resume::TEXT,
  job_candidate__business_entity_id::BIGINT,
  job_candidate__modified_date::DATE,
  job_candidate__record_loaded_at::TIMESTAMP,
  job_candidate__record_updated_at::TIMESTAMP,
  job_candidate__record_version::TEXT,
  job_candidate__record_valid_from::TIMESTAMP,
  job_candidate__record_valid_to::TIMESTAMP,
  job_candidate__is_current_record::BOOLEAN
FROM cte__final