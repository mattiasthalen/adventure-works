MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    sales_reason_id::BIGINT,
    name::TEXT,
    reason_type::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__sales_reasons"
)
;