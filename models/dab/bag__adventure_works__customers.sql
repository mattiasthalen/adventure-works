MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__customer
  ),
  tags hook,
  grain (_pit_hook__customer, _hook__customer),
  description 'Hook viewpoint of customers data: Current customer information. Also see the Person and Store tables.',
  references (_hook__person__customer, _hook__store, _hook__territory__sales),
  column_descriptions (
    customer__customer_id = 'Primary key.',
    customer__store_id = 'Foreign key to Store.BusinessEntityID.',
    customer__territory_id = 'ID of the territory in which the customer is located. Foreign key to SalesTerritory.SalesTerritoryID.',
    customer__account_number = 'Unique number identifying the customer assigned by the accounting system.',
    customer__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    customer__person_id = 'Foreign key to Person.BusinessEntityID.',
    customer__record_loaded_at = 'Timestamp when this record was loaded into the system',
    customer__record_updated_at = 'Timestamp when this record was last updated',
    customer__record_version = 'Version number for this record',
    customer__record_valid_from = 'Timestamp from which this record version is valid',
    customer__record_valid_to = 'Timestamp until which this record version is valid',
    customer__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__customer = 'Reference hook to customer',
    _hook__person__customer = 'Reference hook to customer person',
    _hook__store = 'Reference hook to store',
    _hook__territory__sales = 'Reference hook to sales territory',
    _pit_hook__customer = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    customer_id AS customer__customer_id,
    store_id AS customer__store_id,
    territory_id AS customer__territory_id,
    account_number AS customer__account_number,
    rowguid AS customer__rowguid,
    person_id AS customer__person_id,
    modified_date AS customer__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS customer__record_loaded_at
  FROM das.raw__adventure_works__customers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer__customer_id ORDER BY customer__record_loaded_at) AS customer__record_version,
    CASE
      WHEN customer__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE customer__record_loaded_at
    END AS customer__record_valid_from,
    COALESCE(
      LEAD(customer__record_loaded_at) OVER (PARTITION BY customer__customer_id ORDER BY customer__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS customer__record_valid_to,
    customer__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS customer__is_current_record,
    CASE
      WHEN customer__is_current_record
      THEN customer__record_loaded_at
      ELSE customer__record_valid_to
    END AS customer__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('customer__adventure_works|', customer__customer_id) AS _hook__customer,
    CONCAT('person__customer__adventure_works|', customer__person_id) AS _hook__person__customer,
    CONCAT('store__adventure_works|', customer__store_id) AS _hook__store,
    CONCAT('territory__sales__adventure_works|', customer__territory_id) AS _hook__territory__sales,
    CONCAT_WS('~',
      _hook__customer,
      'epoch__valid_from|'||customer__record_valid_from
    ) AS _pit_hook__customer,
    *
  FROM validity
)
SELECT
  _pit_hook__customer::BLOB,
  _hook__customer::BLOB,
  _hook__person__customer::BLOB,
  _hook__store::BLOB,
  _hook__territory__sales::BLOB,
  customer__customer_id::BIGINT,
  customer__store_id::BIGINT,
  customer__territory_id::BIGINT,
  customer__account_number::TEXT,
  customer__rowguid::TEXT,
  customer__person_id::BIGINT,
  customer__modified_date::DATE,
  customer__record_loaded_at::TIMESTAMP,
  customer__record_updated_at::TIMESTAMP,
  customer__record_version::TEXT,
  customer__record_valid_from::TIMESTAMP,
  customer__record_valid_to::TIMESTAMP,
  customer__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND customer__record_updated_at BETWEEN @start_ts AND @end_ts