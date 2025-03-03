MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    job_candidate_id AS job_candidate__job_candidate_id,
    business_entity_id AS job_candidate__business_entity_id,
    modified_date AS job_candidate__modified_date,
    resume AS job_candidate__resume,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS job_candidate__record_loaded_at
  FROM bronze.raw__adventure_works__job_candidates
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY job_candidate__job_candidate_id ORDER BY job_candidate__record_loaded_at) AS job_candidate__record_version,
    CASE
      WHEN job_candidate__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE job_candidate__record_loaded_at
    END AS job_candidate__record_valid_from,
    COALESCE(
      LEAD(job_candidate__record_loaded_at) OVER (PARTITION BY job_candidate__job_candidate_id ORDER BY job_candidate__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS job_candidate__record_valid_to,
    job_candidate__record_valid_to = @max_ts::TIMESTAMP AS job_candidate__is_current_record,
    CASE
      WHEN job_candidate__is_current_record
      THEN job_candidate__record_loaded_at
      ELSE job_candidate__record_valid_to
    END AS job_candidate__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'job_candidate|adventure_works|',
      job_candidate__job_candidate_id,
      '~epoch|valid_from|',
      job_candidate__record_valid_from
    ) AS _pit_hook__job_candidate,
    CONCAT('job_candidate|adventure_works|', job_candidate__job_candidate_id) AS _hook__job_candidate,
    CONCAT('business_entity|adventure_works|', job_candidate__business_entity_id) AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  _pit_hook__job_candidate::BLOB,
  _hook__job_candidate::BLOB,
  _hook__business_entity::BLOB,
  job_candidate__job_candidate_id::VARCHAR,
  job_candidate__business_entity_id::VARCHAR,
  job_candidate__modified_date::VARCHAR,
  job_candidate__resume::VARCHAR,
  job_candidate__record_loaded_at::TIMESTAMP,
  job_candidate__record_version::INT,
  job_candidate__record_valid_from::TIMESTAMP,
  job_candidate__record_valid_to::TIMESTAMP,
  job_candidate__is_current_record::BOOLEAN,
  job_candidate__record_updated_at::TIMESTAMP
FROM hooks