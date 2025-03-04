MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  illustration_id::BIGINT,
  diagram::VARCHAR,
  modified_date::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__illustrations"
);
