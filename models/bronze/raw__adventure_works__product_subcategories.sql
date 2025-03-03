MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  product_subcategory_id,
  product_category_id,
  modified_date,
  name,
  rowguid,
  _dlt_load_id
  FROM ICEBERG_SCAN(
    "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_subcategories"
  )