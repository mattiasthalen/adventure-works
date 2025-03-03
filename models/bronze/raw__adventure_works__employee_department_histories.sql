MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  department_id,
  end_date,
  modified_date,
  shift_id,
  start_date,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__employee_department_histories"
)
