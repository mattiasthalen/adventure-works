MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__address),
  description 'Business viewpoint of business_entity_addresses data: Cross-reference table mapping customers, vendors, and employees to their addresses.',
  column_descriptions (
    business_entity_address__business_entity_id = 'Primary key. Foreign key to BusinessEntity.BusinessEntityID.',
    business_entity_address__address_id = 'Primary key. Foreign key to Address.AddressID.',
    business_entity_address__address_type_id = 'Primary key. Foreign key to AddressType.AddressTypeID.',
    business_entity_address__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    business_entity_address__modified_date = 'Date when this record was last modified',
    business_entity_address__record_loaded_at = 'Timestamp when this record was loaded into the system',
    business_entity_address__record_updated_at = 'Timestamp when this record was last updated',
    business_entity_address__record_version = 'Version number for this record',
    business_entity_address__record_valid_from = 'Timestamp from which this record version is valid',
    business_entity_address__record_valid_to = 'Timestamp until which this record version is valid',
    business_entity_address__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__address, _hook__business_entity, _hook__reference__address_type)
FROM dab.bag__adventure_works__business_entity_addresses