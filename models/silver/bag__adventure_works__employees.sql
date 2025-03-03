MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS business_entity__business_entity_id,
    login_id AS business_entity__login_id,
    birth_date AS business_entity__birth_date,
    current_flag AS business_entity__current_flag,
    gender AS business_entity__gender,
    hire_date AS business_entity__hire_date,
    job_title AS business_entity__job_title,
    marital_status AS business_entity__marital_status,
    modified_date AS business_entity__modified_date,
    national_idnumber AS business_entity__national_idnumber,
    organization_level AS business_entity__organization_level,
    rowguid AS business_entity__rowguid,
    salaried_flag AS business_entity__salaried_flag,
    sick_leave_hours AS business_entity__sick_leave_hours,
    vacation_hours AS business_entity__vacation_hours,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity__record_loaded_at
  FROM bronze.raw__adventure_works__employees
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity__business_entity_id ORDER BY business_entity__record_loaded_at) AS business_entity__record_version,
    CASE
      WHEN business_entity__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE business_entity__record_loaded_at
    END AS business_entity__record_valid_from,
    COALESCE(
      LEAD(business_entity__record_loaded_at) OVER (PARTITION BY business_entity__business_entity_id ORDER BY business_entity__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS business_entity__record_valid_to,
    business_entity__record_valid_to = @max_ts::TIMESTAMP AS business_entity__is_current_record,
    CASE
      WHEN business_entity__is_current_record
      THEN business_entity__record_loaded_at
      ELSE business_entity__record_valid_to
    END AS business_entity__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      business_entity__business_entity_id,
      '~epoch|valid_from|',
      business_entity__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', business_entity__business_entity_id) AS _hook__business_entity,
    CONCAT('login|adventure_works|', business_entity__login_id) AS _hook__login,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__login::BLOB,
  business_entity__business_entity_id::VARCHAR,
  business_entity__login_id::VARCHAR,
  business_entity__birth_date::VARCHAR,
  business_entity__current_flag::VARCHAR,
  business_entity__gender::VARCHAR,
  business_entity__hire_date::VARCHAR,
  business_entity__job_title::VARCHAR,
  business_entity__marital_status::VARCHAR,
  business_entity__modified_date::VARCHAR,
  business_entity__national_idnumber::VARCHAR,
  business_entity__organization_level::VARCHAR,
  business_entity__rowguid::VARCHAR,
  business_entity__salaried_flag::VARCHAR,
  business_entity__sick_leave_hours::VARCHAR,
  business_entity__vacation_hours::VARCHAR,
  business_entity__record_loaded_at::TIMESTAMP,
  business_entity__record_version::INT,
  business_entity__record_valid_from::TIMESTAMP,
  business_entity__record_valid_to::TIMESTAMP,
  business_entity__is_current_record::BOOLEAN,
  business_entity__record_updated_at::TIMESTAMP
FROM hooks