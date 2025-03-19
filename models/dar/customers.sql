MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__customer),
  description 'Business viewpoint of customers data: Current customer information. Also see the Person and Store tables.',
  column_descriptions (
    customer__customer_id = 'Primary key.',
    customer__store_id = 'Foreign key to Store.BusinessEntityID.',
    customer__territory_id = 'ID of the territory in which the customer is located. Foreign key to SalesTerritory.SalesTerritoryID.',
    customer__account_number = 'Unique number identifying the customer assigned by the accounting system.',
    customer__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    customer__person_id = 'Foreign key to Person.BusinessEntityID.',
    customer__modified_date = 'Date when this record was last modified',
    customer__record_loaded_at = 'Timestamp when this record was loaded into the system',
    customer__record_updated_at = 'Timestamp when this record was last updated',
    customer__record_version = 'Version number for this record',
    customer__record_valid_from = 'Timestamp from which this record version is valid',
    customer__record_valid_to = 'Timestamp until which this record version is valid',
    customer__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__customer,
    customer__customer_id,
    customer__store_id,
    customer__territory_id,
    customer__account_number,
    customer__rowguid,
    customer__person_id,
    customer__modified_date,
    customer__record_loaded_at,
    customer__record_updated_at,
    customer__record_version,
    customer__record_valid_from,
    customer__record_valid_to,
    customer__is_current_record
  FROM dab.bag__adventure_works__customers
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__customer,
    NULL AS customer__customer_id,
    NULL AS customer__store_id,
    NULL AS customer__territory_id,
    'N/A' AS customer__account_number,
    'N/A' AS customer__rowguid,
    NULL AS customer__person_id,
    NULL AS customer__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS customer__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS customer__record_updated_at,
    0 AS customer__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS customer__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS customer__record_valid_to,
    TRUE AS customer__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__customer::BLOB,
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
  customer__is_current_record::BOOLEAN
FROM cte__final