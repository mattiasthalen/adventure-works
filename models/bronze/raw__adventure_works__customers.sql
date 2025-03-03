MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  customer_id,
  account_number,
  modified_date,
  person_id,
  rowguid,
  store_id,
  territory_id,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__customers"
)
