MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    ship_method_id::BIGINT,
    name::TEXT,
    ship_base::DOUBLE,
    ship_rate::DOUBLE,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__ship_methods"
)
;