MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  shift_id,
  end_time,
  modified_date,
  name,
  start_time,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__shifts"
)
