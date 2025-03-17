MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__currency),
  description 'Business viewpoint of currencies data: Lookup table containing standard ISO currencies.',
  column_descriptions (
    currency__currency_code = 'The ISO code for the Currency.',
    currency__name = 'Currency name.',
    currency__modified_date = 'Date when this record was last modified',
    currency__record_loaded_at = 'Timestamp when this record was loaded into the system',
    currency__record_updated_at = 'Timestamp when this record was last updated',
    currency__record_version = 'Version number for this record',
    currency__record_valid_from = 'Timestamp from which this record version is valid',
    currency__record_valid_to = 'Timestamp until which this record version is valid',
    currency__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__currency)
FROM dab.bag__adventure_works__currencies