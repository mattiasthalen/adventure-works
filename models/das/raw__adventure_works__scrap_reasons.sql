MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of scrap_reasons data: Manufacturing failure reasons lookup table.',
  column_descriptions (
    scrap_reason_id = 'Primary key for ScrapReason records.',
    name = 'Failure description.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    scrap_reason_id::BIGINT,
    name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__scrap_reasons"
)
;