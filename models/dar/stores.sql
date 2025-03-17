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

SELECT
  *
  EXCLUDE (_hook__store, _hook__person__sales)
FROM dab.bag__adventure_works__stores