MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__sales_reason),
  description 'Business viewpoint of sales_reasons data: Lookup table of customer purchase reasons.',
  column_descriptions (
    sales_reason__sales_reason_id = 'Primary key for SalesReason records.',
    sales_reason__name = 'Sales reason description.',
    sales_reason__reason_type = 'Category the sales reason belongs to.',
    sales_reason__modified_date = 'Date when this record was last modified',
    sales_reason__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_reason__record_updated_at = 'Timestamp when this record was last updated',
    sales_reason__record_version = 'Version number for this record',
    sales_reason__record_valid_from = 'Timestamp from which this record version is valid',
    sales_reason__record_valid_to = 'Timestamp until which this record version is valid',
    sales_reason__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__sales_reason)
FROM dab.bag__adventure_works__sales_reasons