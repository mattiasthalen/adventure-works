MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of bill_of_materials data: Items required to make bicycles and bicycle subassemblies. It identifies the hierarchical relationship between a parent product and its components.',
  column_descriptions (
    bill_of_materials_id = 'Primary key for BillOfMaterials records.',
    product_assembly_id = 'Parent product identification number. Foreign key to Product.ProductID.',
    component_id = 'Component identification number. Foreign key to Product.ProductID.',
    start_date = 'Date the component started being used in the assembly item.',
    unit_measure_code = 'Standard code identifying the unit of measure for the quantity.',
    bomlevel = 'Indicates the depth the component is from its parent (AssemblyID).',
    per_assembly_qty = 'Quantity of the component needed to create the assembly.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    end_date = 'Date the component stopped being used in the assembly item.'
  )
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