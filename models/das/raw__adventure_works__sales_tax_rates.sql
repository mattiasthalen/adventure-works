MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    sales_tax_rate_id::BIGINT,
    state_province_id::BIGINT,
    tax_type::BIGINT,
    tax_rate::DOUBLE,
    name::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_tax_rates"
)
;