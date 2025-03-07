MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    state_province_id::BIGINT,
    state_province_code::TEXT,
    country_region_code::TEXT,
    is_only_state_province_flag::BOOLEAN,
    name::TEXT,
    territory_id::BIGINT,
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__state_provinces"
)
;