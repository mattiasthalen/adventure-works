MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  account_number,
  active_flag,
  credit_rating,
  modified_date,
  name,
  preferred_vendor_status,
  purchasing_web_service_url,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__vendors"
)
