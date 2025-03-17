MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__job_candidate
  ),
  tags hook,
  grain (_pit_hook__job_candidate, _hook__job_candidate),
  description 'Hook viewpoint of job_candidates data: Résumés submitted to Human Resources by job applicants.',
  references (_hook__person__employee),
  column_descriptions (
    job_candidate__job_candidate_id = 'Primary key for JobCandidate records.',
    job_candidate__resume = 'Résumé in XML format.',
    job_candidate__business_entity_id = 'Employee identification number if applicant was hired. Foreign key to Employee.BusinessEntityID.',
    job_candidate__record_loaded_at = 'Timestamp when this record was loaded into the system',
    job_candidate__record_updated_at = 'Timestamp when this record was last updated',
    job_candidate__record_version = 'Version number for this record',
    job_candidate__record_valid_from = 'Timestamp from which this record version is valid',
    job_candidate__record_valid_to = 'Timestamp until which this record version is valid',
    job_candidate__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__job_candidate = 'Reference hook to job_candidate',
    _hook__person__employee = 'Reference hook to employee person',
    _pit_hook__job_candidate = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    job_candidate_id AS job_candidate__job_candidate_id,
    resume AS job_candidate__resume,
    business_entity_id AS job_candidate__business_entity_id,
    modified_date AS job_candidate__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS job_candidate__record_loaded_at
  FROM das.raw__adventure_works__job_candidates
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY job_candidate__job_candidate_id ORDER BY job_candidate__record_loaded_at) AS job_candidate__record_version,
    CASE
      WHEN job_candidate__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE job_candidate__record_loaded_at
    END AS job_candidate__record_valid_from,
    COALESCE(
      LEAD(job_candidate__record_loaded_at) OVER (PARTITION BY job_candidate__job_candidate_id ORDER BY job_candidate__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS job_candidate__record_valid_to,
    job_candidate__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS job_candidate__is_current_record,
    CASE
      WHEN job_candidate__is_current_record
      THEN job_candidate__record_loaded_at
      ELSE job_candidate__record_valid_to
    END AS job_candidate__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('person__adventure_works|', job_candidate__job_candidate_id) AS _hook__job_candidate,
    CONCAT('person__employee__adventure_works|', job_candidate__business_entity_id) AS _hook__person__employee,
    CONCAT_WS('~',
      _hook__job_candidate,
      'epoch__valid_from|'||job_candidate__record_valid_from
    ) AS _pit_hook__job_candidate,
    *
  FROM validity
)
SELECT
  _pit_hook__job_candidate::BLOB,
  _hook__job_candidate::BLOB,
  _hook__person__employee::BLOB,
  job_candidate__job_candidate_id::BIGINT,
  job_candidate__resume::TEXT,
  job_candidate__business_entity_id::BIGINT,
  job_candidate__modified_date::DATE,
  job_candidate__record_loaded_at::TIMESTAMP,
  job_candidate__record_updated_at::TIMESTAMP,
  job_candidate__record_version::TEXT,
  job_candidate__record_valid_from::TIMESTAMP,
  job_candidate__record_valid_to::TIMESTAMP,
  job_candidate__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND job_candidate__record_updated_at BETWEEN @start_ts AND @end_ts