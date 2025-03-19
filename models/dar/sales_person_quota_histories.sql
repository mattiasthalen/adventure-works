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

WITH cte__source AS (
  SELECT
    _pit_hook__person__sales,
    sales_person_quota_history__business_entity_id,
    sales_person_quota_history__quota_date,
    sales_person_quota_history__sales_quota,
    sales_person_quota_history__rowguid,
    sales_person_quota_history__modified_date,
    sales_person_quota_history__record_loaded_at,
    sales_person_quota_history__record_updated_at,
    sales_person_quota_history__record_version,
    sales_person_quota_history__record_valid_from,
    sales_person_quota_history__record_valid_to,
    sales_person_quota_history__is_current_record
  FROM dab.bag__adventure_works__sales_person_quota_histories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__person__sales,
    NULL AS sales_person_quota_history__business_entity_id,
    NULL AS sales_person_quota_history__quota_date,
    NULL AS sales_person_quota_history__sales_quota,
    'N/A' AS sales_person_quota_history__rowguid,
    NULL AS sales_person_quota_history__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_person_quota_history__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_person_quota_history__record_updated_at,
    0 AS sales_person_quota_history__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_person_quota_history__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS sales_person_quota_history__record_valid_to,
    TRUE AS sales_person_quota_history__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__person__sales::BLOB,
  sales_person_quota_history__business_entity_id::BIGINT,
  sales_person_quota_history__quota_date::DATE,
  sales_person_quota_history__sales_quota::DOUBLE,
  sales_person_quota_history__rowguid::TEXT,
  sales_person_quota_history__modified_date::DATE,
  sales_person_quota_history__record_loaded_at::TIMESTAMP,
  sales_person_quota_history__record_updated_at::TIMESTAMP,
  sales_person_quota_history__record_version::TEXT,
  sales_person_quota_history__record_valid_from::TIMESTAMP,
  sales_person_quota_history__record_valid_to::TIMESTAMP,
  sales_person_quota_history__is_current_record::BOOLEAN
FROM cte__final