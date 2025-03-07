MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    business_entity_id::BIGINT,
    account_number::TEXT,
    name::TEXT,
    credit_rating::BIGINT,
    preferred_vendor_status::BOOLEAN,
    active_flag::BOOLEAN,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    purchasing_web_service_url::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__vendors"
)
;