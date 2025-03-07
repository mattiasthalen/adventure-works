MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    product_id::BIGINT,
    start_date::TIMESTAMP,
    end_date::TEXT,
    standard_cost::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_cost_histories"
)
;