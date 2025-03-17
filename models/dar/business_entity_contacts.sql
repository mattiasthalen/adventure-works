MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__business_entity),
  description 'Business viewpoint of business_entity_contacts data: Cross-reference table mapping stores, vendors, and employees to people.',
  column_descriptions (
    business_entity_contact__business_entity_id = 'Primary key. Foreign key to BusinessEntity.BusinessEntityID.',
    business_entity_contact__person_id = 'Primary key. Foreign key to Person.BusinessEntityID.',
    business_entity_contact__contact_type_id = 'Primary key. Foreign key to ContactType.ContactTypeID.',
    business_entity_contact__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    business_entity_contact__modified_date = 'Date when this record was last modified',
    business_entity_contact__record_loaded_at = 'Timestamp when this record was loaded into the system',
    business_entity_contact__record_updated_at = 'Timestamp when this record was last updated',
    business_entity_contact__record_version = 'Version number for this record',
    business_entity_contact__record_valid_from = 'Timestamp from which this record version is valid',
    business_entity_contact__record_valid_to = 'Timestamp until which this record version is valid',
    business_entity_contact__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__business_entity, _hook__person__contact, _hook__reference__contact_type)
FROM dab.bag__adventure_works__business_entity_contacts