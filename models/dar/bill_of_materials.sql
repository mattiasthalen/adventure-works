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

WITH cte__source AS (
  SELECT
    _pit_hook__bill_of_materials,
    bill_of_material__bill_of_materials_id,
    bill_of_material__product_assembly_id,
    bill_of_material__component_id,
    bill_of_material__start_date,
    bill_of_material__unit_measure_code,
    bill_of_material__bomlevel,
    bill_of_material__per_assembly_qty,
    bill_of_material__end_date,
    bill_of_material__modified_date,
    bill_of_material__record_loaded_at,
    bill_of_material__record_updated_at,
    bill_of_material__record_version,
    bill_of_material__record_valid_from,
    bill_of_material__record_valid_to,
    bill_of_material__is_current_record
  FROM dab.bag__adventure_works__bill_of_materials
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__bill_of_materials,
    NULL AS bill_of_material__bill_of_materials_id,
    NULL AS bill_of_material__product_assembly_id,
    NULL AS bill_of_material__component_id,
    NULL AS bill_of_material__start_date,
    'N/A' AS bill_of_material__unit_measure_code,
    NULL AS bill_of_material__bomlevel,
    NULL AS bill_of_material__per_assembly_qty,
    NULL AS bill_of_material__end_date,
    NULL AS bill_of_material__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS bill_of_material__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS bill_of_material__record_updated_at,
    0 AS bill_of_material__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS bill_of_material__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS bill_of_material__record_valid_to,
    TRUE AS bill_of_material__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__bill_of_materials::BLOB,
  bill_of_material__bill_of_materials_id::BIGINT,
  bill_of_material__product_assembly_id::BIGINT,
  bill_of_material__component_id::BIGINT,
  bill_of_material__start_date::DATE,
  bill_of_material__unit_measure_code::TEXT,
  bill_of_material__bomlevel::BIGINT,
  bill_of_material__per_assembly_qty::DOUBLE,
  bill_of_material__end_date::DATE,
  bill_of_material__modified_date::DATE,
  bill_of_material__record_loaded_at::TIMESTAMP,
  bill_of_material__record_updated_at::TIMESTAMP,
  bill_of_material__record_version::TEXT,
  bill_of_material__record_valid_from::TIMESTAMP,
  bill_of_material__record_valid_to::TIMESTAMP,
  bill_of_material__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.bill_of_materials TO './export/dar/bill_of_materials.parquet' (FORMAT parquet, COMPRESSION zstd)
);