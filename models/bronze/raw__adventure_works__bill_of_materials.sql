MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  bill_of_materials_id,
  bomlevel,
  component_id,
  end_date,
  modified_date,
  per_assembly_qty,
  product_assembly_id,
  start_date,
  unit_measure_code,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__bill_of_materials"
)
