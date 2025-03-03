MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  location_id,
  bin,
  modified_date,
  product_id,
  quantity,
  rowguid,
  shelf,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_inventories"
)
