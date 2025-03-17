MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__country_region),
  description 'Business viewpoint of country_regions data: Lookup table containing the ISO standard codes for countries and regions.',
  column_descriptions (
    country_region__country_region_code = 'ISO standard code for countries and regions.',
    country_region__name = 'Country or region name.',
    country_region__modified_date = 'Date when this record was last modified',
    country_region__record_loaded_at = 'Timestamp when this record was loaded into the system',
    country_region__record_updated_at = 'Timestamp when this record was last updated',
    country_region__record_version = 'Version number for this record',
    country_region__record_valid_from = 'Timestamp from which this record version is valid',
    country_region__record_valid_to = 'Timestamp until which this record version is valid',
    country_region__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__country_region)
FROM dab.bag__adventure_works__country_regions