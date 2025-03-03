MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  scrap_reason_id,
  modified_date,
  name,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__scrap_reasons"
)
