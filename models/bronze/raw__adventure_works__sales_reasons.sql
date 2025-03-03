MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  sales_reason_id,
  modified_date,
  name,
  reason_type,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_reasons"
)
