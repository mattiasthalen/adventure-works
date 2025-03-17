MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__individual
  ),
  tags hook,
  grain (_pit_hook__person__individual, _hook__person__individual),
  description 'Hook viewpoint of persons data: Human beings involved with AdventureWorks: employees, customer contacts, and vendor contacts.',
  column_descriptions (
    person__business_entity_id = 'Primary key for Person records.',
    person__person_type = 'Primary type of person: SC = Store Contact, IN = Individual (retail) customer, SP = Sales person, EM = Employee (non-sales), VC = Vendor contact, GC = General contact.',
    person__name_style = '0 = The data in FirstName and LastName are stored in western style (first name, last name) order. 1 = Eastern style (last name, first name) order.',
    person__first_name = 'First name of the person.',
    person__middle_name = 'Middle name or middle initial of the person.',
    person__last_name = 'Last name of the person.',
    person__email_promotion = '0 = Contact does not wish to receive e-mail promotions, 1 = Contact does wish to receive e-mail promotions from AdventureWorks, 2 = Contact does wish to receive e-mail promotions from AdventureWorks and selected partners.',
    person__demographics = 'Personal information such as hobbies, and income collected from online shoppers. Used for sales analysis.',
    person__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    person__title = 'A courtesy title. For example, Mr. or Ms.',
    person__suffix = 'Surname suffix. For example, Sr. or Jr.',
    person__additional_contact_info = 'Additional contact information about the person stored in xml format.',
    person__record_loaded_at = 'Timestamp when this record was loaded into the system',
    person__record_updated_at = 'Timestamp when this record was last updated',
    person__record_version = 'Version number for this record',
    person__record_valid_from = 'Timestamp from which this record version is valid',
    person__record_valid_to = 'Timestamp until which this record version is valid',
    person__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__person__individual = 'Reference hook to individual person',
    _pit_hook__person__individual = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    business_entity_id AS person__business_entity_id,
    person_type AS person__person_type,
    name_style AS person__name_style,
    first_name AS person__first_name,
    middle_name AS person__middle_name,
    last_name AS person__last_name,
    email_promotion AS person__email_promotion,
    demographics AS person__demographics,
    rowguid AS person__rowguid,
    title AS person__title,
    suffix AS person__suffix,
    additional_contact_info AS person__additional_contact_info,
    modified_date AS person__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS person__record_loaded_at
  FROM das.raw__adventure_works__persons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY person__business_entity_id ORDER BY person__record_loaded_at) AS person__record_version,
    CASE
      WHEN person__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE person__record_loaded_at
    END AS person__record_valid_from,
    COALESCE(
      LEAD(person__record_loaded_at) OVER (PARTITION BY person__business_entity_id ORDER BY person__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS person__record_valid_to,
    person__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS person__is_current_record,
    CASE
      WHEN person__is_current_record
      THEN person__record_loaded_at
      ELSE person__record_valid_to
    END AS person__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('person__individual__adventure_works|', person__business_entity_id) AS _hook__person__individual,
    CONCAT_WS('~',
      _hook__person__individual,
      'epoch__valid_from|'||person__record_valid_from
    ) AS _pit_hook__person__individual,
    *
  FROM validity
)
SELECT
  _pit_hook__person__individual::BLOB,
  _hook__person__individual::BLOB,
  person__business_entity_id::BIGINT,
  person__person_type::TEXT,
  person__name_style::BOOLEAN,
  person__first_name::TEXT,
  person__middle_name::TEXT,
  person__last_name::TEXT,
  person__email_promotion::BIGINT,
  person__demographics::TEXT,
  person__rowguid::TEXT,
  person__title::TEXT,
  person__suffix::TEXT,
  person__additional_contact_info::TEXT,
  person__modified_date::DATE,
  person__record_loaded_at::TIMESTAMP,
  person__record_updated_at::TIMESTAMP,
  person__record_version::TEXT,
  person__record_valid_from::TIMESTAMP,
  person__record_valid_to::TIMESTAMP,
  person__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND person__record_updated_at BETWEEN @start_ts AND @end_ts