MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__location),
  description 'Business viewpoint of locations data: Product inventory and manufacturing locations.',
  column_descriptions (
    location__location_id = 'Primary key for Location records.',
    location__name = 'Location description.',
    location__cost_rate = 'Standard hourly cost of the manufacturing location.',
    location__availability = 'Work capacity (in hours) of the manufacturing location.',
    location__modified_date = 'Date when this record was last modified',
    location__record_loaded_at = 'Timestamp when this record was loaded into the system',
    location__record_updated_at = 'Timestamp when this record was last updated',
    location__record_version = 'Version number for this record',
    location__record_valid_from = 'Timestamp from which this record version is valid',
    location__record_valid_to = 'Timestamp until which this record version is valid',
    location__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__location)
FROM dab.bag__adventure_works__locations