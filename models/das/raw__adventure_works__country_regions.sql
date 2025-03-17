MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of country_regions data: Lookup table containing the ISO standard codes for countries and regions.',
  column_descriptions (
    country_region_code = 'ISO standard code for countries and regions.',
    name = 'Country or region name.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    country_region_code::TEXT,
    name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__country_regions"
)
;