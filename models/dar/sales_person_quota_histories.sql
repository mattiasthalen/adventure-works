MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__sales),
  description 'Business viewpoint of sales_person_quota_histories data: Sales performance tracking.',
  column_descriptions (
    sales_person_quota_history__business_entity_id = 'Sales person identification number. Foreign key to SalesPerson.BusinessEntityID.',
    sales_person_quota_history__quota_date = 'Sales quota date.',
    sales_person_quota_history__sales_quota = 'Sales quota amount.',
    sales_person_quota_history__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_person_quota_history__modified_date = 'Date when this record was last modified',
    sales_person_quota_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_person_quota_history__record_updated_at = 'Timestamp when this record was last updated',
    sales_person_quota_history__record_version = 'Version number for this record',
    sales_person_quota_history__record_valid_from = 'Timestamp from which this record version is valid',
    sales_person_quota_history__record_valid_to = 'Timestamp until which this record version is valid',
    sales_person_quota_history__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__person__sales)
FROM dab.bag__adventure_works__sales_person_quota_histories