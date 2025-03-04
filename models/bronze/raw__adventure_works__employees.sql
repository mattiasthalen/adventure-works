MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  login_id::VARCHAR,
  birth_date::VARCHAR,
  current_flag::BOOLEAN,
  gender::VARCHAR,
  hire_date::VARCHAR,
  job_title::VARCHAR,
  marital_status::VARCHAR,
  modified_date::VARCHAR,
  national_idnumber::VARCHAR,
  organization_level::BIGINT,
  rowguid::VARCHAR,
  salaried_flag::BOOLEAN,
  sick_leave_hours::BIGINT,
  vacation_hours::BIGINT,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__employees"
);
