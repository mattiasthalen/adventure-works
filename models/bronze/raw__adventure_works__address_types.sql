MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  address_type_id,
  modified_date,
  name,
  rowguid,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__address_types"
)
