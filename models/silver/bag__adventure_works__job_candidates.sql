MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column job_candidat__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    job_candidate_id AS job_candidat__job_candidate_id,
    business_entity_id AS job_candidat__business_entity_id,
    modified_date AS job_candidat__modified_date,
    resume AS job_candidat__resume,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS job_candidat__record_loaded_at
  FROM bronze.raw__adventure_works__job_candidates
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY job_candidat__job_candidate_id ORDER BY job_candidat__record_loaded_at) AS job_candidat__record_version,
    CASE
      WHEN job_candidat__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE job_candidat__record_loaded_at
    END AS job_candidat__record_valid_from,
    COALESCE(
      LEAD(job_candidat__record_loaded_at) OVER (PARTITION BY job_candidat__job_candidate_id ORDER BY job_candidat__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS job_candidat__record_valid_to,
    job_candidat__record_valid_to = @max_ts::TIMESTAMP AS job_candidat__is_current_record,
    CASE
      WHEN job_candidat__is_current_record
      THEN job_candidat__record_loaded_at
      ELSE job_candidat__record_valid_to
    END AS job_candidat__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'job_candidate|adventure_works|',
      job_candidat__job_candidate_id,
      '~epoch|valid_from|',
      job_candidat__record_valid_from
    ) AS _pit_hook__job_candidate,
    CONCAT('job_candidate|adventure_works|', job_candidat__job_candidate_id) AS _hook__job_candidate,
    CONCAT('business_entity|adventure_works|', job_candidat__business_entity_id) AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  _pit_hook__job_candidate::BLOB,
  _hook__job_candidate::BLOB,
  _hook__business_entity::BLOB,
  job_candidat__job_candidate_id::BIGINT,
  job_candidat__business_entity_id::BIGINT,
  job_candidat__modified_date::VARCHAR,
  job_candidat__resume::VARCHAR,
  job_candidat__record_loaded_at::TIMESTAMP,
  job_candidat__record_updated_at::TIMESTAMP,
  job_candidat__record_valid_from::TIMESTAMP,
  job_candidat__record_valid_to::TIMESTAMP,
  job_candidat__record_version::INT,
  job_candidat__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND job_candidat__record_updated_at BETWEEN @start_ts AND @end_ts