MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of currencies data: Lookup table containing standard ISO currencies.',
  column_descriptions (
    currency_code = 'The ISO code for the Currency.',
    name = 'Currency name.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    currency_code::TEXT,
    name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__currencies"
)
;