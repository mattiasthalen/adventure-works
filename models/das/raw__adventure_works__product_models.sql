MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    product_model_id::BIGINT,
    name::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    catalog_description::TEXT,
    instructions::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_models"
)
;