MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__illustration),
  description 'Business viewpoint of illustrations data: Bicycle assembly diagrams.',
  column_descriptions (
    illustration__illustration_id = 'Primary key for Illustration records.',
    illustration__diagram = 'Illustrations used in manufacturing instructions. Stored as XML.',
    illustration__modified_date = 'Date when this record was last modified',
    illustration__record_loaded_at = 'Timestamp when this record was loaded into the system',
    illustration__record_updated_at = 'Timestamp when this record was last updated',
    illustration__record_version = 'Version number for this record',
    illustration__record_valid_from = 'Timestamp from which this record version is valid',
    illustration__record_valid_to = 'Timestamp until which this record version is valid',
    illustration__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__illustration)
FROM dab.bag__adventure_works__illustrations