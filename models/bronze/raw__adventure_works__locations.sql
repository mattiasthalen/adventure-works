MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  location_id,
  availability,
  cost_rate,
  modified_date,
  name,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__locations"
)
