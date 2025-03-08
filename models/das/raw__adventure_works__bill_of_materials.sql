MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    bill_of_materials_id::BIGINT,
    product_assembly_id::BIGINT,
    component_id::BIGINT,
    start_date::DATE,
    unit_measure_code::TEXT,
    bomlevel::BIGINT,
    per_assembly_qty::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    end_date::DATE
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__bill_of_materials"
)
;