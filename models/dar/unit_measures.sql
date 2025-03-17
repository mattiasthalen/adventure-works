MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__unit_measure),
  description 'Business viewpoint of unit_measures data: Unit of measure lookup table.',
  column_descriptions (
    unit_measure__unit_measure_code = 'Primary key.',
    unit_measure__name = 'Unit of measure description.',
    unit_measure__modified_date = 'Date when this record was last modified',
    unit_measure__record_loaded_at = 'Timestamp when this record was loaded into the system',
    unit_measure__record_updated_at = 'Timestamp when this record was last updated',
    unit_measure__record_version = 'Version number for this record',
    unit_measure__record_valid_from = 'Timestamp from which this record version is valid',
    unit_measure__record_valid_to = 'Timestamp until which this record version is valid',
    unit_measure__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__unit_measure)
FROM dab.bag__adventure_works__unit_measures