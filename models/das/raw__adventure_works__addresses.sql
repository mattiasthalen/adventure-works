MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    address_id::BIGINT,
    address_line1::TEXT,
    city::TEXT,
    state_province_id::BIGINT,
    postal_code::TEXT,
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    address_line2::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__addresses"
)
;