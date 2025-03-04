MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  shift_id::BIGINT,
  end_time::VARCHAR,
  modified_date::VARCHAR,
  name::VARCHAR,
  start_time::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__shifts"
);
