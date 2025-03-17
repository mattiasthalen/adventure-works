MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of sales_person_quota_histories data: Sales performance tracking.',
  column_descriptions (
    business_entity_id = 'Sales person identification number. Foreign key to SalesPerson.BusinessEntityID.',
    quota_date = 'Sales quota date.',
    sales_quota = 'Sales quota amount.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    business_entity_id::BIGINT,
    quota_date::DATE,
    sales_quota::DOUBLE,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__sales_person_quota_histories"
)
;