MODEL (
  kind VIEW,
  enabled TRUE
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
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    title::TEXT,
    suffix::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__persons"
)
;