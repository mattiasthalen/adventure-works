MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  department_id,
  group_name,
  modified_date,
  name,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__departments"
)
