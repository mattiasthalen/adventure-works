MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of persons data: Human beings involved with AdventureWorks: employees, customer contacts, and vendor contacts.',
  column_descriptions (
    business_entity_id = 'Primary key for Person records.',
    person_type = 'Primary type of person: SC = Store Contact, IN = Individual (retail) customer, SP = Sales person, EM = Employee (non-sales), VC = Vendor contact, GC = General contact.',
    name_style = '0 = The data in FirstName and LastName are stored in western style (first name, last name) order. 1 = Eastern style (last name, first name) order.',
    first_name = 'First name of the person.',
    middle_name = 'Middle name or middle initial of the person.',
    last_name = 'Last name of the person.',
    email_promotion = '0 = Contact does not wish to receive e-mail promotions, 1 = Contact does wish to receive e-mail promotions from AdventureWorks, 2 = Contact does wish to receive e-mail promotions from AdventureWorks and selected partners.',
    demographics = 'Personal information such as hobbies, and income collected from online shoppers. Used for sales analysis.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    title = 'A courtesy title. For example, Mr. or Ms.',
    suffix = 'Surname suffix. For example, Sr. or Jr.',
    additional_contact_info = 'Additional contact information about the person stored in xml format.'
  )
);

SELECT
    business_entity_id::BIGINT,
    person_type::TEXT,
    name_style::BOOLEAN,
    first_name::TEXT,
    middle_name::TEXT,
    last_name::TEXT,
    email_promotion::BIGINT,
    demographics::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    title::TEXT,
    suffix::TEXT,
    additional_contact_info::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__persons"
)
;