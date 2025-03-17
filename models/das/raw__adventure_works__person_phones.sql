MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of person_phones data: Telephone number and type of a person.',
  column_descriptions (
    business_entity_id = 'Business entity identification number. Foreign key to Person.BusinessEntityID.',
    phone_number = 'Telephone number identification number.',
    phone_number_type_id = 'Kind of phone number. Foreign key to PhoneNumberType.PhoneNumberTypeID.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    business_entity_id::BIGINT,
    phone_number::TEXT,
    phone_number_type_id::BIGINT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__person_phones"
)
;