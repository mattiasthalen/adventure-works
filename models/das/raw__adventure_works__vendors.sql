MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of vendors data: Companies from whom Adventure Works Cycles purchases parts or other goods.',
  column_descriptions (
    business_entity_id = 'Primary key for Vendor records. Foreign key to BusinessEntity.BusinessEntityID.',
    account_number = 'Vendor account (identification) number.',
    name = 'Company name.',
    credit_rating = '1 = Superior, 2 = Excellent, 3 = Above average, 4 = Average, 5 = Below average.',
    preferred_vendor_status = '0 = Do not use if another vendor is available. 1 = Preferred over other vendors supplying the same product.',
    active_flag = '0 = Vendor no longer used. 1 = Vendor is actively used.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    purchasing_web_service_url = 'Vendor URL.'
  )
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
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__vendors"
)
;