MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  birth_date,
  current_flag,
  gender,
  hire_date,
  job_title,
  login_id,
  marital_status,
  modified_date,
  national_idnumber,
  organization_level,
  rowguid,
  salaried_flag,
  sick_leave_hours,
  vacation_hours,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__employees"
)
