MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    bill_of_materials_id::BIGINT,
    product_assembly_id::BIGINT,
    component_id::BIGINT,
    start_date::TEXT,
    unit_measure_code::TEXT,
    bomlevel::BIGINT,
    per_assembly_qty::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    end_date::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__bill_of_materials"
)
;