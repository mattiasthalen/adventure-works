MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  currency_code,
  modified_date,
  name,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__currencies"
)
