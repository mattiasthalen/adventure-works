MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__shift),
  description 'Business viewpoint of shifts data: Work shift lookup table.',
  column_descriptions (
    shift__shift_id = 'Primary key for Shift records.',
    shift__name = 'Shift description.',
    shift__start_time = 'Shift start time. ISO duration.',
    shift__end_time = 'Shift end time. ISO duration.',
    shift__modified_date = 'Date when this record was last modified',
    shift__record_loaded_at = 'Timestamp when this record was loaded into the system',
    shift__record_updated_at = 'Timestamp when this record was last updated',
    shift__record_version = 'Version number for this record',
    shift__record_valid_from = 'Timestamp from which this record version is valid',
    shift__record_valid_to = 'Timestamp until which this record version is valid',
    shift__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__shift)
FROM dab.bag__adventure_works__shifts