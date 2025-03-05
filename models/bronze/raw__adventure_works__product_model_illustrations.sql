MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    product_model_id::BIGINT,
    illustration_id::BIGINT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_model_illustrations"
)
;