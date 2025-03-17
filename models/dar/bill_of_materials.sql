MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__bill_of_materials),
  description 'Business viewpoint of bill_of_materials data: Items required to make bicycles and bicycle subassemblies. It identifies the hierarchical relationship between a parent product and its components.',
  column_descriptions (
    bill_of_material__bill_of_materials_id = 'Primary key for BillOfMaterials records.',
    bill_of_material__product_assembly_id = 'Parent product identification number. Foreign key to Product.ProductID.',
    bill_of_material__component_id = 'Component identification number. Foreign key to Product.ProductID.',
    bill_of_material__start_date = 'Date the component started being used in the assembly item.',
    bill_of_material__unit_measure_code = 'Standard code identifying the unit of measure for the quantity.',
    bill_of_material__bomlevel = 'Indicates the depth the component is from its parent (AssemblyID).',
    bill_of_material__per_assembly_qty = 'Quantity of the component needed to create the assembly.',
    bill_of_material__end_date = 'Date the component stopped being used in the assembly item.',
    bill_of_material__modified_date = 'Date when this record was last modified',
    bill_of_material__record_loaded_at = 'Timestamp when this record was loaded into the system',
    bill_of_material__record_updated_at = 'Timestamp when this record was last updated',
    bill_of_material__record_version = 'Version number for this record',
    bill_of_material__record_valid_from = 'Timestamp from which this record version is valid',
    bill_of_material__record_valid_to = 'Timestamp until which this record version is valid',
    bill_of_material__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__bill_of_materials, _hook__product__assembly, _hook__product__component, _hook__reference__unit_measure)
FROM dab.bag__adventure_works__bill_of_materials