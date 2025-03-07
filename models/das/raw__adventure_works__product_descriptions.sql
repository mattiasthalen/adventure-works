MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    product_description_id::BIGINT,
    description::TEXT,
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_descriptions"
)
;