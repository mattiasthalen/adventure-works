MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of illustrations data: Bicycle assembly diagrams.',
  column_descriptions (
    illustration_id = 'Primary key for Illustration records.',
    diagram = 'Illustrations used in manufacturing instructions. Stored as XML.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    illustration_id::BIGINT,
    diagram::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__illustrations"
)
;