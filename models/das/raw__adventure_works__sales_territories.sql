MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    territory_id::BIGINT,
    name::TEXT,
    country_region_code::TEXT,
    group::TEXT,
    sales_ytd::DOUBLE,
    sales_last_year::DOUBLE,
    cost_ytd::DOUBLE,
    cost_last_year::DOUBLE,
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_territories"
)
;