MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  department_id::BIGINT,
  shift_id::BIGINT,
  end_date::VARCHAR,
  modified_date::VARCHAR,
  start_date::TIMESTAMP,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__employee_department_histories"
);
