MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__culture),
  description 'Business viewpoint of cultures data: Lookup table containing the languages in which some AdventureWorks data is stored.',
  column_descriptions (
    culture__culture_id = 'Primary key for Culture records.',
    culture__name = 'Culture description.',
    culture__modified_date = 'Date when this record was last modified',
    culture__record_loaded_at = 'Timestamp when this record was loaded into the system',
    culture__record_updated_at = 'Timestamp when this record was last updated',
    culture__record_version = 'Version number for this record',
    culture__record_valid_from = 'Timestamp from which this record version is valid',
    culture__record_valid_to = 'Timestamp until which this record version is valid',
    culture__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__culture)
FROM dab.bag__adventure_works__cultures