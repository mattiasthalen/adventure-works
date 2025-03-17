MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__business_entity
  ),
  tags hook,
  grain (_pit_hook__business_entity, _hook__business_entity),
  description 'Hook viewpoint of business_entity_contacts data: Cross-reference table mapping stores, vendors, and employees to people.',
  references (_hook__person__contact, _hook__reference__contact_type),
  column_descriptions (
    business_entity_contact__business_entity_id = 'Primary key. Foreign key to BusinessEntity.BusinessEntityID.',
    business_entity_contact__person_id = 'Primary key. Foreign key to Person.BusinessEntityID.',
    business_entity_contact__contact_type_id = 'Primary key. Foreign key to ContactType.ContactTypeID.',
    business_entity_contact__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    business_entity_contact__record_loaded_at = 'Timestamp when this record was loaded into the system',
    business_entity_contact__record_updated_at = 'Timestamp when this record was last updated',
    business_entity_contact__record_version = 'Version number for this record',
    business_entity_contact__record_valid_from = 'Timestamp from which this record version is valid',
    business_entity_contact__record_valid_to = 'Timestamp until which this record version is valid',
    business_entity_contact__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__business_entity = 'Reference hook to business_entity',
    _hook__person__contact = 'Reference hook to contact person',
    _hook__reference__contact_type = 'Reference hook to contact_type reference',
    _pit_hook__business_entity = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    business_entity_id AS business_entity_contact__business_entity_id,
    person_id AS business_entity_contact__person_id,
    contact_type_id AS business_entity_contact__contact_type_id,
    rowguid AS business_entity_contact__rowguid,
    modified_date AS business_entity_contact__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity_contact__record_loaded_at
  FROM das.raw__adventure_works__business_entity_contacts
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity_contact__business_entity_id ORDER BY business_entity_contact__record_loaded_at) AS business_entity_contact__record_version,
    CASE
      WHEN business_entity_contact__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE business_entity_contact__record_loaded_at
    END AS business_entity_contact__record_valid_from,
    COALESCE(
      LEAD(business_entity_contact__record_loaded_at) OVER (PARTITION BY business_entity_contact__business_entity_id ORDER BY business_entity_contact__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS business_entity_contact__record_valid_to,
    business_entity_contact__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS business_entity_contact__is_current_record,
    CASE
      WHEN business_entity_contact__is_current_record
      THEN business_entity_contact__record_loaded_at
      ELSE business_entity_contact__record_valid_to
    END AS business_entity_contact__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('business_entity__adventure_works|', business_entity_contact__business_entity_id) AS _hook__business_entity,
    CONCAT('person__individual__adventure_works|', business_entity_contact__person_id) AS _hook__person__contact,
    CONCAT('reference__contact_type__adventure_works|', business_entity_contact__contact_type_id) AS _hook__reference__contact_type,
    CONCAT_WS('~',
      _hook__business_entity,
      'epoch__valid_from|'||business_entity_contact__record_valid_from
    ) AS _pit_hook__business_entity,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__person__contact::BLOB,
  _hook__reference__contact_type::BLOB,
  business_entity_contact__business_entity_id::BIGINT,
  business_entity_contact__person_id::BIGINT,
  business_entity_contact__contact_type_id::BIGINT,
  business_entity_contact__rowguid::TEXT,
  business_entity_contact__modified_date::DATE,
  business_entity_contact__record_loaded_at::TIMESTAMP,
  business_entity_contact__record_updated_at::TIMESTAMP,
  business_entity_contact__record_version::TEXT,
  business_entity_contact__record_valid_from::TIMESTAMP,
  business_entity_contact__record_valid_to::TIMESTAMP,
  business_entity_contact__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND business_entity_contact__record_updated_at BETWEEN @start_ts AND @end_ts