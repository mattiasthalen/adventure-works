MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  bill_of_materials_id::BIGINT,
  component_id::BIGINT,
  product_assembly_id::BIGINT,
  bomlevel::BIGINT,
  end_date::VARCHAR,
  modified_date::VARCHAR,
  per_assembly_qty::DOUBLE,
  start_date::VARCHAR,
  unit_measure_code::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__bill_of_materials"
);
