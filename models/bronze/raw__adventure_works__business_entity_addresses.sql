MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  address_id,
  address_type_id,
  business_entity_id,
  modified_date,
  rowguid,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__business_entity_addresses"
)
