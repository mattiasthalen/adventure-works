MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__store),
  description 'Business viewpoint of stores data: Customers (resellers) of Adventure Works products.',
  column_descriptions (
    store__business_entity_id = 'Primary key. Foreign key to Customer.BusinessEntityID.',
    store__name = 'Name of the store.',
    store__sales_person_id = 'ID of the sales person assigned to the customer. Foreign key to SalesPerson.BusinessEntityID.',
    store__demographics = 'Demographic information about the store such as the number of employees, annual sales and store type.',
    store__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    store__modified_date = 'Date when this record was last modified',
    store__record_loaded_at = 'Timestamp when this record was loaded into the system',
    store__record_updated_at = 'Timestamp when this record was last updated',
    store__record_version = 'Version number for this record',
    store__record_valid_from = 'Timestamp from which this record version is valid',
    store__record_valid_to = 'Timestamp until which this record version is valid',
    store__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__store,
    store__business_entity_id,
    store__name,
    store__sales_person_id,
    store__demographics,
    store__rowguid,
    store__modified_date,
    store__record_loaded_at,
    store__record_updated_at,
    store__record_version,
    store__record_valid_from,
    store__record_valid_to,
    store__is_current_record
  FROM dab.bag__adventure_works__stores
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__store,
    NULL AS store__business_entity_id,
    'N/A' AS store__name,
    NULL AS store__sales_person_id,
    'N/A' AS store__demographics,
    'N/A' AS store__rowguid,
    NULL AS store__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS store__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS store__record_updated_at,
    0 AS store__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS store__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS store__record_valid_to,
    TRUE AS store__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__store::BLOB,
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
  store__is_current_record::BOOLEAN
FROM cte__final