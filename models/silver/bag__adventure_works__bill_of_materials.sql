MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    bill_of_materials_id AS bill_of_materials__bill_of_materials_id,
    component_id AS bill_of_materials__component_id,
    product_assembly_id AS bill_of_materials__product_assembly_id,
    bomlevel AS bill_of_materials__bomlevel,
    end_date AS bill_of_materials__end_date,
    modified_date AS bill_of_materials__modified_date,
    per_assembly_qty AS bill_of_materials__per_assembly_qty,
    start_date AS bill_of_materials__start_date,
    unit_measure_code AS bill_of_materials__unit_measure_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS bill_of_materials__record_loaded_at
  FROM bronze.raw__adventure_works__bill_of_materials
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY bill_of_materials__bill_of_materials_id ORDER BY bill_of_materials__record_loaded_at) AS bill_of_materials__record_version,
    CASE
      WHEN bill_of_materials__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE bill_of_materials__record_loaded_at
    END AS bill_of_materials__record_valid_from,
    COALESCE(
      LEAD(bill_of_materials__record_loaded_at) OVER (PARTITION BY bill_of_materials__bill_of_materials_id ORDER BY bill_of_materials__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS bill_of_materials__record_valid_to,
    bill_of_materials__record_valid_to = @max_ts::TIMESTAMP AS bill_of_materials__is_current_record,
    CASE
      WHEN bill_of_materials__is_current_record
      THEN bill_of_materials__record_loaded_at
      ELSE bill_of_materials__record_valid_to
    END AS bill_of_materials__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'bill_of_materials|adventure_works|',
      bill_of_materials__bill_of_materials_id,
      '~epoch|valid_from|',
      bill_of_materials__record_valid_from
    ) AS _pit_hook__bill_of_materials,
    CONCAT('bill_of_materials|adventure_works|', bill_of_materials__bill_of_materials_id) AS _hook__bill_of_materials,
    CONCAT('component|adventure_works|', bill_of_materials__component_id) AS _hook__component,
    CONCAT('product_assembly|adventure_works|', bill_of_materials__product_assembly_id) AS _hook__product_assembly,
    *
  FROM validity
)
SELECT
  _pit_hook__bill_of_materials::BLOB,
  _hook__bill_of_materials::BLOB,
  _hook__component::BLOB,
  _hook__product_assembly::BLOB,
  bill_of_materials__bill_of_materials_id::VARCHAR,
  bill_of_materials__component_id::VARCHAR,
  bill_of_materials__product_assembly_id::VARCHAR,
  bill_of_materials__bomlevel::VARCHAR,
  bill_of_materials__end_date::VARCHAR,
  bill_of_materials__modified_date::VARCHAR,
  bill_of_materials__per_assembly_qty::VARCHAR,
  bill_of_materials__start_date::VARCHAR,
  bill_of_materials__unit_measure_code::VARCHAR,
  bill_of_materials__record_loaded_at::TIMESTAMP,
  bill_of_materials__record_version::INT,
  bill_of_materials__record_valid_from::TIMESTAMP,
  bill_of_materials__record_valid_to::TIMESTAMP,
  bill_of_materials__is_current_record::BOOLEAN,
  bill_of_materials__record_updated_at::TIMESTAMP
FROM hooks