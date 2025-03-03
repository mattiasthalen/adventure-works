MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  product_description_id,
  description,
  modified_date,
  rowguid,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_descriptions"
)
