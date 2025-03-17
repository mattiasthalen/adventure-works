MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of unit_measures data: Unit of measure lookup table.',
  column_descriptions (
    unit_measure_code = 'Primary key.',
    name = 'Unit of measure description.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    unit_measure_code::TEXT,
    name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__unit_measures"
)
;