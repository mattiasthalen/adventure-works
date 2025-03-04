MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bill_of_material__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    bill_of_materials_id AS bill_of_material__bill_of_materials_id,
    component_id AS bill_of_material__component_id,
    product_assembly_id AS bill_of_material__product_assembly_id,
    bomlevel AS bill_of_material__bomlevel,
    end_date AS bill_of_material__end_date,
    modified_date AS bill_of_material__modified_date,
    per_assembly_qty AS bill_of_material__per_assembly_qty,
    start_date AS bill_of_material__start_date,
    unit_measure_code AS bill_of_material__unit_measure_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS bill_of_material__record_loaded_at
  FROM bronze.raw__adventure_works__bill_of_materials
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY bill_of_material__bill_of_materials_id ORDER BY bill_of_material__record_loaded_at) AS bill_of_material__record_version,
    CASE
      WHEN bill_of_material__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE bill_of_material__record_loaded_at
    END AS bill_of_material__record_valid_from,
    COALESCE(
      LEAD(bill_of_material__record_loaded_at) OVER (PARTITION BY bill_of_material__bill_of_materials_id ORDER BY bill_of_material__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS bill_of_material__record_valid_to,
    bill_of_material__record_valid_to = @max_ts::TIMESTAMP AS bill_of_material__is_current_record,
    CASE
      WHEN bill_of_material__is_current_record
      THEN bill_of_material__record_loaded_at
      ELSE bill_of_material__record_valid_to
    END AS bill_of_material__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'bill_of_materials|adventure_works|',
      bill_of_material__bill_of_materials_id,
      '~epoch|valid_from|',
      bill_of_material__record_valid_from
    ) AS _pit_hook__bill_of_materials,
    CONCAT('bill_of_materials|adventure_works|', bill_of_material__bill_of_materials_id) AS _hook__bill_of_materials,
    CONCAT('product_assembly|adventure_works|', bill_of_material__product_assembly_id) AS _hook__product_assembly,
    CONCAT('component|adventure_works|', bill_of_material__component_id) AS _hook__component,
    *
  FROM validity
)
SELECT
  _pit_hook__bill_of_materials::BLOB,
  _hook__bill_of_materials::BLOB,
  _hook__component::BLOB,
  _hook__product_assembly::BLOB,
  bill_of_material__bill_of_materials_id::BIGINT,
  bill_of_material__component_id::BIGINT,
  bill_of_material__product_assembly_id::BIGINT,
  bill_of_material__bomlevel::BIGINT,
  bill_of_material__end_date::VARCHAR,
  bill_of_material__modified_date::VARCHAR,
  bill_of_material__per_assembly_qty::DOUBLE,
  bill_of_material__start_date::VARCHAR,
  bill_of_material__unit_measure_code::VARCHAR,
  bill_of_material__record_loaded_at::TIMESTAMP,
  bill_of_material__record_updated_at::TIMESTAMP,
  bill_of_material__record_valid_from::TIMESTAMP,
  bill_of_material__record_valid_to::TIMESTAMP,
  bill_of_material__record_version::INT,
  bill_of_material__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND bill_of_material__record_updated_at BETWEEN @start_ts AND @end_ts