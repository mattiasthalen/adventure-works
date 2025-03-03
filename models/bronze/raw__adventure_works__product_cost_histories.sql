MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  product_id,
  end_date,
  modified_date,
  standard_cost,
  start_date,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_cost_histories"
)
