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

SELECT
  *
  EXCLUDE (_hook__customer, _hook__person__customer, _hook__store, _hook__territory__sales)
FROM dab.bag__adventure_works__customers