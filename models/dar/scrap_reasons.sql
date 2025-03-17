MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__scrap_reason),
  description 'Business viewpoint of scrap_reasons data: Manufacturing failure reasons lookup table.',
  column_descriptions (
    scrap_reason__scrap_reason_id = 'Primary key for ScrapReason records.',
    scrap_reason__name = 'Failure description.',
    scrap_reason__modified_date = 'Date when this record was last modified',
    scrap_reason__record_loaded_at = 'Timestamp when this record was loaded into the system',
    scrap_reason__record_updated_at = 'Timestamp when this record was last updated',
    scrap_reason__record_version = 'Version number for this record',
    scrap_reason__record_valid_from = 'Timestamp from which this record version is valid',
    scrap_reason__record_valid_to = 'Timestamp until which this record version is valid',
    scrap_reason__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__scrap_reason)
FROM dab.bag__adventure_works__scrap_reasons