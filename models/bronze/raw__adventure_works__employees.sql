MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    business_entity_id::BIGINT,
    national_idnumber::TEXT,
    login_id::TEXT,
    job_title::TEXT,
    birth_date::TEXT,
    marital_status::TEXT,
    gender::TEXT,
    hire_date::TEXT,
    salaried_flag::BOOLEAN,
    vacation_hours::BIGINT,
    sick_leave_hours::BIGINT,
    current_flag::BOOLEAN,
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    organization_level::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__employees"
)
;