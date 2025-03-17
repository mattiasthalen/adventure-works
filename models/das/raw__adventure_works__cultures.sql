MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of cultures data: Lookup table containing the languages in which some AdventureWorks data is stored.',
  column_descriptions (
    culture_id = 'Primary key for Culture records.',
    name = 'Culture description.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    culture_id::TEXT,
    name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__cultures"
)
;