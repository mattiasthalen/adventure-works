MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  email,
  email_address_id,
  modified_date,
  rowguid,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__email_addresses"
)
