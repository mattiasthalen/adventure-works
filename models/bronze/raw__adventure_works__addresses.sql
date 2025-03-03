MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    address_id,
    state_province_id,
    address_line1,
    address_line2,
    city,
    modified_date,
    postal_code,
    rowguid,
    _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__addresses"
)
;