MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    business_entity_id::BIGINT,
    national_idnumber::TEXT,
    login_id::TEXT,
    job_title::TEXT,
    birth_date::DATE,
    marital_status::TEXT,
    gender::TEXT,
    hire_date::DATE,
    salaried_flag::BOOLEAN,
    vacation_hours::BIGINT,
    sick_leave_hours::BIGINT,
    current_flag::BOOLEAN,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    organization_level::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__employees"
)
;