MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  sales_tax_rate_id,
  modified_date,
  name,
  rowguid,
  state_province_id,
  tax_rate,
  tax_type,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_tax_rates"
)
