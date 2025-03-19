MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__individual),
  description 'Business viewpoint of persons data: Human beings involved with AdventureWorks: employees, customer contacts, and vendor contacts.',
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
    person__modified_date = 'Date when this record was last modified',
    person__record_loaded_at = 'Timestamp when this record was loaded into the system',
    person__record_updated_at = 'Timestamp when this record was last updated',
    person__record_version = 'Version number for this record',
    person__record_valid_from = 'Timestamp from which this record version is valid',
    person__record_valid_to = 'Timestamp until which this record version is valid',
    person__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__person__individual,
    person__business_entity_id,
    person__person_type,
    person__name_style,
    person__first_name,
    person__middle_name,
    person__last_name,
    person__email_promotion,
    person__demographics,
    person__rowguid,
    person__title,
    person__suffix,
    person__additional_contact_info,
    person__modified_date,
    person__record_loaded_at,
    person__record_updated_at,
    person__record_version,
    person__record_valid_from,
    person__record_valid_to,
    person__is_current_record
  FROM dab.bag__adventure_works__persons
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__person__individual,
    NULL AS person__business_entity_id,
    'N/A' AS person__person_type,
    FALSE AS person__name_style,
    'N/A' AS person__first_name,
    'N/A' AS person__middle_name,
    'N/A' AS person__last_name,
    NULL AS person__email_promotion,
    'N/A' AS person__demographics,
    'N/A' AS person__rowguid,
    'N/A' AS person__title,
    'N/A' AS person__suffix,
    'N/A' AS person__additional_contact_info,
    NULL AS person__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS person__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS person__record_updated_at,
    0 AS person__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS person__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS person__record_valid_to,
    TRUE AS person__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__person__individual::BLOB,
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
  person__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.persons TO './export/dar/persons.parquet' (FORMAT parquet, COMPRESSION zstd)
);