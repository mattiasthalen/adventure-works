MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    customer_id::BIGINT,
    store_id::BIGINT,
    territory_id::BIGINT,
    account_number::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    person_id::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__customers"
)
;