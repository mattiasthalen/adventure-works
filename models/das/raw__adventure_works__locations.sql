MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of locations data: Product inventory and manufacturing locations.',
  column_descriptions (
    location_id = 'Primary key for Location records.',
    name = 'Location description.',
    cost_rate = 'Standard hourly cost of the manufacturing location.',
    availability = 'Work capacity (in hours) of the manufacturing location.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    location_id::BIGINT,
    name::TEXT,
    cost_rate::DOUBLE,
    availability::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__locations"
)
;