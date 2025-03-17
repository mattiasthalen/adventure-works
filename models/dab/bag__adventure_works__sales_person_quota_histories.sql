MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__sales
  ),
  tags hook,
  grain (_pit_hook__person__sales, _hook__person__sales),
  description 'Hook viewpoint of sales_person_quota_histories data: Sales performance tracking.',
  column_descriptions (
    sales_person_quota_history__business_entity_id = 'Sales person identification number. Foreign key to SalesPerson.BusinessEntityID.',
    sales_person_quota_history__quota_date = 'Sales quota date.',
    sales_person_quota_history__sales_quota = 'Sales quota amount.',
    sales_person_quota_history__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_person_quota_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_person_quota_history__record_updated_at = 'Timestamp when this record was last updated',
    sales_person_quota_history__record_version = 'Version number for this record',
    sales_person_quota_history__record_valid_from = 'Timestamp from which this record version is valid',
    sales_person_quota_history__record_valid_to = 'Timestamp until which this record version is valid',
    sales_person_quota_history__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__person__sales = 'Reference hook to sales person',
    _pit_hook__person__sales = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    business_entity_id AS sales_person_quota_history__business_entity_id,
    quota_date AS sales_person_quota_history__quota_date,
    sales_quota AS sales_person_quota_history__sales_quota,
    rowguid AS sales_person_quota_history__rowguid,
    modified_date AS sales_person_quota_history__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_person_quota_history__record_loaded_at
  FROM das.raw__adventure_works__sales_person_quota_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_person_quota_history__business_entity_id ORDER BY sales_person_quota_history__record_loaded_at) AS sales_person_quota_history__record_version,
    CASE
      WHEN sales_person_quota_history__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_person_quota_history__record_loaded_at
    END AS sales_person_quota_history__record_valid_from,
    COALESCE(
      LEAD(sales_person_quota_history__record_loaded_at) OVER (PARTITION BY sales_person_quota_history__business_entity_id ORDER BY sales_person_quota_history__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_person_quota_history__record_valid_to,
    sales_person_quota_history__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_person_quota_history__is_current_record,
    CASE
      WHEN sales_person_quota_history__is_current_record
      THEN sales_person_quota_history__record_loaded_at
      ELSE sales_person_quota_history__record_valid_to
    END AS sales_person_quota_history__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('person__sales__adventure_works|', sales_person_quota_history__business_entity_id) AS _hook__person__sales,
    CONCAT_WS('~',
      _hook__person__sales,
      'epoch__valid_from|'||sales_person_quota_history__record_valid_from
    ) AS _pit_hook__person__sales,
    *
  FROM validity
)
SELECT
  _pit_hook__person__sales::BLOB,
  _hook__person__sales::BLOB,
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
  sales_person_quota_history__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND sales_person_quota_history__record_updated_at BETWEEN @start_ts AND @end_ts