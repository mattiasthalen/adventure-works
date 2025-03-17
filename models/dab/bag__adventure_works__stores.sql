MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__store
  ),
  tags hook,
  grain (_pit_hook__store, _hook__store),
  description 'Hook viewpoint of stores data: Customers (resellers) of Adventure Works products.',
  references (_hook__person__sales),
  column_descriptions (
    store__business_entity_id = 'Primary key. Foreign key to Customer.BusinessEntityID.',
    store__name = 'Name of the store.',
    store__sales_person_id = 'ID of the sales person assigned to the customer. Foreign key to SalesPerson.BusinessEntityID.',
    store__demographics = 'Demographic information about the store such as the number of employees, annual sales and store type.',
    store__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    store__record_loaded_at = 'Timestamp when this record was loaded into the system',
    store__record_updated_at = 'Timestamp when this record was last updated',
    store__record_version = 'Version number for this record',
    store__record_valid_from = 'Timestamp from which this record version is valid',
    store__record_valid_to = 'Timestamp until which this record version is valid',
    store__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__store = 'Reference hook to store',
    _hook__person__sales = 'Reference hook to sales person',
    _pit_hook__store = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    business_entity_id AS store__business_entity_id,
    name AS store__name,
    sales_person_id AS store__sales_person_id,
    demographics AS store__demographics,
    rowguid AS store__rowguid,
    modified_date AS store__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS store__record_loaded_at
  FROM das.raw__adventure_works__stores
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY store__business_entity_id ORDER BY store__record_loaded_at) AS store__record_version,
    CASE
      WHEN store__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE store__record_loaded_at
    END AS store__record_valid_from,
    COALESCE(
      LEAD(store__record_loaded_at) OVER (PARTITION BY store__business_entity_id ORDER BY store__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS store__record_valid_to,
    store__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS store__is_current_record,
    CASE
      WHEN store__is_current_record
      THEN store__record_loaded_at
      ELSE store__record_valid_to
    END AS store__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('store__adventure_works|', store__business_entity_id) AS _hook__store,
    CONCAT('person__sales__adventure_works|', store__sales_person_id) AS _hook__person__sales,
    CONCAT_WS('~',
      _hook__store,
      'epoch__valid_from|'||store__record_valid_from
    ) AS _pit_hook__store,
    *
  FROM validity
)
SELECT
  _pit_hook__store::BLOB,
  _hook__store::BLOB,
  _hook__person__sales::BLOB,
  store__business_entity_id::BIGINT,
  store__name::TEXT,
  store__sales_person_id::BIGINT,
  store__demographics::TEXT,
  store__rowguid::TEXT,
  store__modified_date::DATE,
  store__record_loaded_at::TIMESTAMP,
  store__record_updated_at::TIMESTAMP,
  store__record_version::TEXT,
  store__record_valid_from::TIMESTAMP,
  store__record_valid_to::TIMESTAMP,
  store__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND store__record_updated_at BETWEEN @start_ts AND @end_ts