MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of sales_reasons data: Lookup table of customer purchase reasons.',
  column_descriptions (
    sales_reason_id = 'Primary key for SalesReason records.',
    name = 'Sales reason description.',
    reason_type = 'Category the sales reason belongs to.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
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