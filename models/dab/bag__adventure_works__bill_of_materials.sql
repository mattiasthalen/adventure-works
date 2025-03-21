MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bill_of_materials
  ),
  tags hook,
  grain (_pit_hook__bill_of_materials, _hook__bill_of_materials),
  description 'Hook viewpoint of bill_of_materials data: Items required to make bicycles and bicycle subassemblies. It identifies the hierarchical relationship between a parent product and its components.',
  references (_hook__product__assembly, _hook__product__component, _hook__reference__unit_measure),
  column_descriptions (
    bill_of_material__bill_of_materials_id = 'Primary key for BillOfMaterials records.',
    bill_of_material__product_assembly_id = 'Parent product identification number. Foreign key to Product.ProductID.',
    bill_of_material__component_id = 'Component identification number. Foreign key to Product.ProductID.',
    bill_of_material__start_date = 'Date the component started being used in the assembly item.',
    bill_of_material__unit_measure_code = 'Standard code identifying the unit of measure for the quantity.',
    bill_of_material__bomlevel = 'Indicates the depth the component is from its parent (AssemblyID).',
    bill_of_material__per_assembly_qty = 'Quantity of the component needed to create the assembly.',
    bill_of_material__end_date = 'Date the component stopped being used in the assembly item.',
    bill_of_material__record_loaded_at = 'Timestamp when this record was loaded into the system',
    bill_of_material__record_updated_at = 'Timestamp when this record was last updated',
    bill_of_material__record_version = 'Version number for this record',
    bill_of_material__record_valid_from = 'Timestamp from which this record version is valid',
    bill_of_material__record_valid_to = 'Timestamp until which this record version is valid',
    bill_of_material__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__bill_of_materials = 'Reference hook to bill_of_materials',
    _hook__product__assembly = 'Reference hook to assembly product',
    _hook__product__component = 'Reference hook to component product',
    _hook__reference__unit_measure = 'Reference hook to unit_measure reference',
    _pit_hook__bill_of_materials = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    bill_of_materials_id AS bill_of_material__bill_of_materials_id,
    product_assembly_id AS bill_of_material__product_assembly_id,
    component_id AS bill_of_material__component_id,
    start_date AS bill_of_material__start_date,
    unit_measure_code AS bill_of_material__unit_measure_code,
    bomlevel AS bill_of_material__bomlevel,
    per_assembly_qty AS bill_of_material__per_assembly_qty,
    end_date AS bill_of_material__end_date,
    modified_date AS bill_of_material__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS bill_of_material__record_loaded_at
  FROM das.raw__adventure_works__bill_of_materials
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY bill_of_material__bill_of_materials_id ORDER BY bill_of_material__record_loaded_at) AS bill_of_material__record_version,
    CASE
      WHEN bill_of_material__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE bill_of_material__record_loaded_at
    END AS bill_of_material__record_valid_from,
    COALESCE(
      LEAD(bill_of_material__record_loaded_at) OVER (PARTITION BY bill_of_material__bill_of_materials_id ORDER BY bill_of_material__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS bill_of_material__record_valid_to,
    bill_of_material__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bill_of_material__is_current_record,
    CASE
      WHEN bill_of_material__is_current_record
      THEN bill_of_material__record_loaded_at
      ELSE bill_of_material__record_valid_to
    END AS bill_of_material__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product__adventure_works|', bill_of_material__bill_of_materials_id) AS _hook__bill_of_materials,
    CONCAT('product__adventure_works|', bill_of_material__product_assembly_id) AS _hook__product__assembly,
    CONCAT('product__adventure_works|', bill_of_material__component_id) AS _hook__product__component,
    CONCAT('reference__unit_measure__adventure_works|', bill_of_material__unit_measure_code) AS _hook__reference__unit_measure,
    CONCAT_WS('~',
      _hook__bill_of_materials,
      'epoch__valid_from|'||bill_of_material__record_valid_from
    ) AS _pit_hook__bill_of_materials,
    *
  FROM validity
)
SELECT
  _pit_hook__bill_of_materials::BLOB,
  _hook__bill_of_materials::BLOB,
  _hook__product__assembly::BLOB,
  _hook__product__component::BLOB,
  _hook__reference__unit_measure::BLOB,
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
  bill_of_material__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND bill_of_material__record_updated_at BETWEEN @start_ts AND @end_ts