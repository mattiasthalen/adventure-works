MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column employe__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS employe__business_entity_id,
    login_id AS employe__login_id,
    birth_date AS employe__birth_date,
    current_flag AS employe__current_flag,
    gender AS employe__gender,
    hire_date AS employe__hire_date,
    job_title AS employe__job_title,
    marital_status AS employe__marital_status,
    modified_date AS employe__modified_date,
    national_idnumber AS employe__national_idnumber,
    organization_level AS employe__organization_level,
    rowguid AS employe__rowguid,
    salaried_flag AS employe__salaried_flag,
    sick_leave_hours AS employe__sick_leave_hours,
    vacation_hours AS employe__vacation_hours,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS employe__record_loaded_at
  FROM bronze.raw__adventure_works__employees
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY employe__business_entity_id ORDER BY employe__record_loaded_at) AS employe__record_version,
    CASE
      WHEN employe__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE employe__record_loaded_at
    END AS employe__record_valid_from,
    COALESCE(
      LEAD(employe__record_loaded_at) OVER (PARTITION BY employe__business_entity_id ORDER BY employe__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS employe__record_valid_to,
    employe__record_valid_to = @max_ts::TIMESTAMP AS employe__is_current_record,
    CASE
      WHEN employe__is_current_record
      THEN employe__record_loaded_at
      ELSE employe__record_valid_to
    END AS employe__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      employe__business_entity_id,
      '~epoch|valid_from|',
      employe__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', employe__business_entity_id) AS _hook__business_entity,
    CONCAT('login|adventure_works|', HEX(employe__login_id)) AS _hook__login,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__login::BLOB,
  employe__business_entity_id::BIGINT,
  employe__login_id::VARCHAR,
  employe__birth_date::VARCHAR,
  employe__current_flag::BOOLEAN,
  employe__gender::VARCHAR,
  employe__hire_date::VARCHAR,
  employe__job_title::VARCHAR,
  employe__marital_status::VARCHAR,
  employe__modified_date::VARCHAR,
  employe__national_idnumber::VARCHAR,
  employe__organization_level::BIGINT,
  employe__rowguid::VARCHAR,
  employe__salaried_flag::BOOLEAN,
  employe__sick_leave_hours::BIGINT,
  employe__vacation_hours::BIGINT,
  employe__record_loaded_at::TIMESTAMP,
  employe__record_updated_at::TIMESTAMP,
  employe__record_valid_from::TIMESTAMP,
  employe__record_valid_to::TIMESTAMP,
  employe__record_version::INT,
  employe__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND employe__record_updated_at BETWEEN @start_ts AND @end_ts