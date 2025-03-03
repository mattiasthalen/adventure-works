MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  illustration_id,
  diagram,
  modified_date,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__illustrations"
)
